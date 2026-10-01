package org.streamhouseoss.flink.catalog;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * A catalog with one database (the Kafka cluster) whose tables are the cluster's topics. Every
 * topic with a registered {@code <topic>-value} Avro schema is a table, with no DDL. CREATE TABLE
 * and CTAS create the topic and register its schemas; DROP TABLE deletes them; ALTER TABLE SET
 * ('changelog.mode' = ...) switches a topic between append and upsert by changing its cleanup policy.
 */
public class KafkaTopicCatalog extends AbstractCatalog {

    static final String DEFINITIONS_TOPIC = "_streamhouse.flink-tables";

    private final String bootstrapServers;
    private final String registryUrl;
    private final Map<String, String> kafkaProperties;
    private final int defaultPartitions;
    private final short replicationFactor;

    private Admin admin;
    private SchemaRegistry registry;
    private TableDefinitions definitions;

    public KafkaTopicCatalog(String name, String database, String bootstrapServers, String registryUrl,
            Map<String, String> kafkaProperties, int defaultPartitions, short replicationFactor) {
        super(name, database);
        this.bootstrapServers = bootstrapServers;
        this.registryUrl = registryUrl;
        this.kafkaProperties = Map.copyOf(kafkaProperties);
        this.defaultPartitions = defaultPartitions;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public void open() {
        Properties client = new Properties();
        client.putAll(kafkaProperties);
        client.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        client.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        client.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        // The Kafka clients are relocated inside this jar; keep their class loading self-contained.
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(KafkaTopicCatalog.class.getClassLoader());
        try {
            admin = Admin.create(client);
            createTopic(DEFINITIONS_TOPIC, 1, "compact", Optional.empty());
            definitions = new TableDefinitions(DEFINITIONS_TOPIC, client);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        registry = new SchemaRegistry(registryUrl);
    }

    @Override
    public void close() {
        if (definitions != null) {
            definitions.close();
        }
        if (admin != null) {
            admin.close();
        }
    }

    // ---- databases: exactly one, the cluster --------------------------------------------------

    @Override
    public List<String> listDatabases() {
        return List.of(getDefaultDatabase());
    }

    @Override
    public CatalogDatabase getDatabase(String name) throws DatabaseNotExistException {
        requireDatabase(name);
        return new CatalogDatabaseImpl(Map.of(), "Kafka cluster " + bootstrapServers);
    }

    @Override
    public boolean databaseExists(String name) {
        return getDefaultDatabase().equals(name);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) {
        throw new CatalogException("this catalog has exactly one database (" + getDefaultDatabase() + "), the Kafka cluster");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
        throw new CatalogException("the database " + getDefaultDatabase() + " is the Kafka cluster and cannot be dropped");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw new CatalogException("databases cannot be altered");
    }

    // ---- tables: topics -----------------------------------------------------------------------

    @Override
    public List<String> listTables(String database) throws DatabaseNotExistException {
        requireDatabase(database);
        Set<String> subjects = registry.subjects();
        return topics().stream()
                .filter(t -> subjects.contains(t + "-value") || definitions.get(t).isPresent())
                .sorted()
                .toList();
    }

    @Override
    public List<String> listViews(String database) throws DatabaseNotExistException {
        requireDatabase(database);
        return List.of();
    }

    @Override
    public boolean tableExists(ObjectPath path) {
        return databaseExists(path.getDatabaseName()) && topics().contains(path.getObjectName())
                && (registry.latest(path.getObjectName() + "-value").isPresent() || definitions.get(path.getObjectName()).isPresent());
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath path) throws TableNotExistException {
        String topic = path.getObjectName();
        if (!databaseExists(path.getDatabaseName()) || !topics().contains(topic)) {
            throw new TableNotExistException(getName(), path);
        }
        return TableTranslator.toCatalogTable(topic, spec(path), bootstrapServers, registryUrl, kafkaProperties);
    }

    /** The table as currently defined by the topic, its schemas and any stored definition. */
    TableSpec spec(ObjectPath path) throws TableNotExistException {
        String topic = path.getObjectName();
        Optional<String> value = registry.latest(topic + "-value");
        if (value.isEmpty()) {
            throw new TableNotExistException(getName(), path,
                    new CatalogException("topic " + topic + " has no registered value schema (subject " + topic + "-value)"));
        }
        boolean compacted = cleanupPolicy(topic).contains("compact");
        Optional<TableDefinitions.Definition> stored = definitions.get(topic);
        if (stored.isEmpty()) {
            return TableTranslator.infer(registry.latest(topic + "-key"), value.get(), compacted);
        }
        TableDefinitions.Definition d = stored.get();
        TableSpec inferred = TableTranslator.infer(Optional.empty(), value.get(), false);
        List<String> keyNames = d.keyColumns().stream().map(TableSpec.Column::name).toList();
        List<TableSpec.Column> valueColumns = d.valueIncludesKey()
                ? inferred.valueColumns().stream()
                        .map(c -> keyNames.contains(c.name()) ? d.keyColumns().get(keyNames.indexOf(c.name())) : c).toList()
                : inferred.valueColumns();
        String mode = compacted && !d.keyColumns().isEmpty() ? TableSpec.UPSERT : TableSpec.APPEND;
        return new TableSpec(d.keyColumns(), valueColumns, d.keyFormat(), d.valueFormat(), d.keyFieldsPrefix(),
                d.valueIncludesKey(), mode, d.startupMode());
    }

    @Override
    public void createTable(ObjectPath path, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        requireDatabase(path.getDatabaseName());
        String topic = path.getObjectName();
        if (!(table instanceof ResolvedCatalogTable resolved)) {
            throw new CatalogException("only tables can be created in this catalog (views and models are not supported)");
        }
        if (topics().contains(topic)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(getName(), path);
        }
        TableTranslator.Creation creation = TableTranslator.fromCreate(resolved, defaultPartitions);
        TableSpec spec = creation.spec();
        TableTranslator.TopicSettings settings = creation.topic();
        createTopic(topic, settings.partitions(), settings.cleanupPolicy(), settings.retentionMs());
        TableTranslator.avroSchemas(spec).forEach((side, schema) -> registry.register(topic + "-" + side, schema));
        definitions.put(topic, new TableDefinitions.Definition(spec.keyColumns(), spec.keyFormat(), spec.keyFieldsPrefix(),
                spec.valueIncludesKey(), spec.valueFormat(), spec.startupMode()));
    }

    @Override
    public void dropTable(ObjectPath path, boolean ignoreIfNotExists) throws TableNotExistException {
        String topic = path.getObjectName();
        if (!databaseExists(path.getDatabaseName()) || !topics().contains(topic)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(getName(), path);
        }
        try {
            admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new CatalogException("cannot delete topic " + topic + ": " + e.getCause().getMessage(), e);
            }
        } catch (Exception e) {
            throw new CatalogException("cannot delete topic " + topic + ": " + e.getMessage(), e);
        }
        registry.delete(topic + "-key");
        registry.delete(topic + "-value");
        if (definitions.get(topic).isPresent()) {
            definitions.remove(topic);
        }
    }

    @Override
    public void renameTable(ObjectPath path, String newName, boolean ignoreIfNotExists) {
        throw new CatalogException("Kafka topics cannot be renamed");
    }

    @Override
    public void alterTable(ObjectPath path, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException {
        CatalogBaseTable current = getTable(path);
        Map<String, String> changed = new java.util.HashMap<>(newTable.getOptions());
        current.getOptions().forEach((k, v) -> {
            if (v.equals(changed.get(k))) {
                changed.remove(k);
            }
        });
        applyOptionChanges(path, changed);
    }

    @Override
    public void alterTable(ObjectPath path, CatalogBaseTable newTable, List<TableChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {
        if (!tableExists(path)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(getName(), path);
        }
        Map<String, String> set = new java.util.HashMap<>();
        for (TableChange change : changes) {
            if (change instanceof TableChange.SetOption option) {
                set.put(option.getKey(), option.getValue());
            } else {
                throw new CatalogException("only ALTER TABLE ... SET ('changelog.mode' = 'append' | 'upsert') is supported, got "
                        + change);
            }
        }
        applyOptionChanges(path, set);
    }

    /** Only the changelog mode can change: it is the topic's cleanup policy (upsert = compact). */
    private void applyOptionChanges(ObjectPath path, Map<String, String> changed) throws TableNotExistException {
        if (changed.isEmpty()) {
            return;
        }
        if (changed.size() != 1 || !changed.containsKey("changelog.mode")) {
            throw new CatalogException("only 'changelog.mode' can be changed, got " + changed.keySet());
        }
        String mode = changed.get("changelog.mode").toLowerCase(java.util.Locale.ROOT);
        String policy = switch (mode) {
            case TableSpec.UPSERT -> {
                TableSpec spec = spec(path);
                if (spec.keyColumns().isEmpty()) {
                    throw new CatalogException("upsert needs a key; topic " + path.getObjectName() + " has none");
                }
                if (TableSpec.AVRO_DEBEZIUM.equals(spec.valueFormat())) {
                    throw new CatalogException("Debezium-envelope topics are read as retract changelogs and cannot be upsert");
                }
                yield TopicConfig.CLEANUP_POLICY_COMPACT;
            }
            case TableSpec.APPEND -> TopicConfig.CLEANUP_POLICY_DELETE;
            default -> throw new CatalogException("'changelog.mode' can be set to 'append' or 'upsert', got '" + mode + "'");
        };
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, path.getObjectName());
        try {
            admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                    new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, policy), AlterConfigOp.OpType.SET))))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new CatalogException("cannot change cleanup.policy of " + path.getObjectName() + ": " + e.getMessage(), e);
        }
    }

    // ---- Kafka helpers ------------------------------------------------------------------------

    private Set<String> topics() {
        try {
            return admin.listTopics().names().get(15, TimeUnit.SECONDS).stream()
                    .filter(t -> !t.startsWith("_") && !t.startsWith("connect-"))
                    .collect(java.util.stream.Collectors.toSet());
        } catch (Exception e) {
            throw new CatalogException("cannot list Kafka topics: " + e.getMessage(), e);
        }
    }

    private String cleanupPolicy(String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try {
            ConfigEntry entry = admin.describeConfigs(List.of(resource)).all().get(15, TimeUnit.SECONDS)
                    .get(resource).get(TopicConfig.CLEANUP_POLICY_CONFIG);
            return entry == null || entry.value() == null ? TopicConfig.CLEANUP_POLICY_DELETE : entry.value();
        } catch (Exception e) {
            throw new CatalogException("cannot describe topic " + topic + ": " + e.getMessage(), e);
        }
    }

    private void createTopic(String topic, int partitions, String cleanupPolicy, Optional<Long> retentionMs) {
        Map<String, String> configs = new java.util.HashMap<>();
        configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
        retentionMs.ifPresent(ms -> configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(ms)));
        NewTopic newTopic = new NewTopic(topic, Optional.of(partitions),
                replicationFactor > 0 ? Optional.of(replicationFactor) : Optional.empty()).configs(configs);
        try {
            admin.createTopics(List.of(newTopic)).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new CatalogException("cannot create topic " + topic + ": " + e.getCause().getMessage(), e);
            }
        } catch (Exception e) {
            throw new CatalogException("cannot create topic " + topic + ": " + e.getMessage(), e);
        }
    }

    private void requireDatabase(String name) throws DatabaseNotExistException {
        if (!databaseExists(name)) {
            throw new DatabaseNotExistException(getName(), name);
        }
    }

    // ---- unsupported: partitions, functions, statistics ---------------------------------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath path) throws TableNotPartitionedException {
        throw new TableNotPartitionedException(getName(), path);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath path, CatalogPartitionSpec spec) throws TableNotPartitionedException {
        throw new TableNotPartitionedException(getName(), path);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath path, List<Expression> filters)
            throws TableNotPartitionedException {
        throw new TableNotPartitionedException(getName(), path);
    }

    @Override
    public CatalogPartition getPartition(ObjectPath path, CatalogPartitionSpec spec) throws PartitionNotExistException {
        throw new PartitionNotExistException(getName(), path, spec);
    }

    @Override
    public boolean partitionExists(ObjectPath path, CatalogPartitionSpec spec) {
        return false;
    }

    @Override
    public void createPartition(ObjectPath path, CatalogPartitionSpec spec, CatalogPartition partition, boolean ignoreIfExists) {
        throw new CatalogException("tables in this catalog are not partitioned");
    }

    @Override
    public void dropPartition(ObjectPath path, CatalogPartitionSpec spec, boolean ignoreIfNotExists) {
        throw new CatalogException("tables in this catalog are not partitioned");
    }

    @Override
    public void alterPartition(ObjectPath path, CatalogPartitionSpec spec, CatalogPartition partition, boolean ignoreIfNotExists) {
        throw new CatalogException("tables in this catalog are not partitioned");
    }

    @Override
    public List<String> listFunctions(String database) {
        return List.of();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath path) throws FunctionNotExistException {
        throw new FunctionNotExistException(getName(), path);
    }

    @Override
    public boolean functionExists(ObjectPath path) {
        return false;
    }

    @Override
    public void createFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfExists) {
        throw new CatalogException("functions are not supported in this catalog; use temporary functions");
    }

    @Override
    public void alterFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfNotExists) {
        throw new CatalogException("functions are not supported in this catalog");
    }

    @Override
    public void dropFunction(ObjectPath path, boolean ignoreIfNotExists) {
        throw new CatalogException("functions are not supported in this catalog");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath path) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath path) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath path, CatalogPartitionSpec spec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath path, CatalogPartitionSpec spec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath path, CatalogTableStatistics statistics, boolean ignoreIfNotExists) {
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath path, CatalogColumnStatistics statistics, boolean ignoreIfNotExists) {
    }

    @Override
    public void alterPartitionStatistics(ObjectPath path, CatalogPartitionSpec spec, CatalogTableStatistics statistics,
            boolean ignoreIfNotExists) {
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath path, CatalogPartitionSpec spec, CatalogColumnStatistics statistics,
            boolean ignoreIfNotExists) {
    }

    static Collection<String> supportedOptions() {
        return TableTranslator.SUPPORTED_OPTIONS;
    }
}
