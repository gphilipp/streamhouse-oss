package org.streamhouseoss.controlplane.reconcile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.streamhouseoss.controlplane.StreamhouseConfig;
import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.ConnectClient;
import org.streamhouseoss.controlplane.clients.KafkaTopics;
import org.streamhouseoss.controlplane.clients.SchemaRegistryApi;
import org.streamhouseoss.controlplane.clients.Secrets;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.OptionValue;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;
import org.streamhouseoss.model.TableRef;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * CDC sources become Debezium connectors on Kafka Connect: one compacted topic per table, keyed by
 * primary key, flattened rows, deletes as tombstones, Avro in the Confluent wire format.
 */
@ApplicationScoped
public class SourceReconciler implements Reconciler {

    private static final Logger LOG = Logger.getLogger(SourceReconciler.class);

    private final ConnectClient connect;
    private final KafkaTopics topics;
    private final SchemaRegistryApi registry;
    private final Secrets secrets;
    private final StreamhouseConfig.Internal internal;

    public SourceReconciler(ConnectClient connect, KafkaTopics topics, @RestClient SchemaRegistryApi registry, Secrets secrets,
            StreamhouseConfig config) {
        this.connect = connect;
        this.topics = topics;
        this.registry = registry;
        this.secrets = secrets;
        this.internal = config.internal();
    }

    @Override
    public ResourceKind kind() {
        return ResourceKind.SOURCE;
    }

    static String connectorName(String source) {
        return "source-" + source;
    }

    static String slotName(String source) {
        return "streamhouse_" + source;
    }

    @Override
    public Outcome reconcile(StoredResource stored, Topology topology) {
        Resource.Source source = (Resource.Source) stored.resource();
        Optional<Resource.Connection> connection = topology.connection(source.connection());
        if (connection.isEmpty()) {
            return Outcome.pending("waiting for connection " + source.connection());
        }
        String name = connectorName(source.name());
        Optional<ConnectClient.ConnectorStatus> status = connect.status(name);
        if (status.isEmpty() || stored.observedGeneration() < stored.generation()) {
            for (TableRef table : source.tables()) {
                topics.ensure(source.topicFor(table), true);
            }
            connect.put(name, connectorConfig(source, connection.get()));
            return Outcome.pending("starting connector " + name);
        }
        ConnectClient.ConnectorStatus s = status.get();
        if (s.failedTaskTrace() != null) {
            connect.restartFailed(name);
            return Outcome.failed("connector " + name + " failed: " + firstLine(s.failedTaskTrace()));
        }
        if (!s.running()) {
            return Outcome.pending("connector " + name + " is " + s.connectorState());
        }
        return Outcome.ready("capturing " + source.tables().size() + " table(s) with connector " + name,
                Map.of("connector", name, "topics", source.tables().stream().map(source::topicFor).toList()));
    }

    Map<String, String> connectorConfig(Resource.Source source, Resource.Connection connection) {
        Map<String, OptionValue> options = connection.options();
        Map<String, String> config = new LinkedHashMap<>();
        config.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("tasks.max", "1");
        config.put("database.hostname", secrets.resolve(options, "host"));
        config.put("database.port", port(options));
        config.put("database.user", secrets.resolve(options, "user"));
        config.put("database.password", secrets.resolve(options, "password"));
        config.put("database.dbname", secrets.resolve(options, "database"));
        config.put("topic.prefix", source.name());
        config.put("plugin.name", "pgoutput");
        if (options.containsKey("publication")) {
            config.put("publication.name", secrets.resolve(options, "publication"));
            config.put("publication.autocreate.mode", "disabled");
        } else {
            config.put("publication.name", "streamhouse_" + source.name());
            config.put("publication.autocreate.mode", "filtered");
        }
        config.put("slot.name", slotName(source.name()));
        config.put("table.include.list", source.tables().stream().map(TableRef::toString).collect(Collectors.joining(",")));
        config.put("snapshot.mode", "initial");
        config.put("decimal.handling.mode", "double");
        config.put("time.precision.mode", "connect");
        config.put("transforms", "unwrap");
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.drop.tombstones", "false");
        config.put("transforms.unwrap.delete.tombstone.handling.mode", "tombstone");
        for (String side : List.of("key", "value")) {
            config.put(side + ".converter", "io.apicurio.registry.utils.converter.AvroConverter");
            config.put(side + ".converter.apicurio.registry.url", internal.apicurioUrl());
            config.put(side + ".converter.apicurio.registry.auto-register", "true");
            config.put(side + ".converter.apicurio.registry.find-latest", "true");
            config.put(side + ".converter.apicurio.registry.headers.enabled", "false");
            config.put(side + ".converter.apicurio.registry.id-handler", "io.apicurio.registry.serde.Default4ByteIdHandler");
            config.put(side + ".converter.apicurio.registry.use-id", "contentId");
        }
        return config;
    }

    @Override
    public boolean delete(StoredResource stored, Topology topology) {
        Resource.Source source = (Resource.Source) stored.resource();
        connect.delete(connectorName(source.name()));
        dropReplicationSlot(source, topology);
        List<String> sourceTopics = source.tables().stream().map(source::topicFor).toList();
        topics.delete(sourceTopics);
        sourceTopics.forEach(t -> {
            for (String subject : List.of(t + "-key", t + "-value")) {
                registry.delete(subject, false);
                registry.delete(subject, true);
            }
        });
        return true;
    }

    /** Without this the source database retains WAL for the abandoned slot forever. */
    private void dropReplicationSlot(Resource.Source source, Topology topology) {
        Optional<Resource.Connection> connection = topology.connection(source.connection());
        if (connection.isEmpty()) {
            LOG.warnf("Cannot drop replication slot %s: connection %s is gone", slotName(source.name()), source.connection());
            return;
        }
        Map<String, OptionValue> o = connection.get().options();
        String url = "jdbc:postgresql://" + secrets.resolve(o, "host") + ":" + port(o) + "/" + secrets.resolve(o, "database");
        try (Connection c = DriverManager.getConnection(url, secrets.resolve(o, "user"), secrets.resolve(o, "password"));
                PreparedStatement ps = c.prepareStatement(
                        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = ? AND NOT active")) {
            ps.setString(1, slotName(source.name()));
            ps.execute();
        } catch (SQLException | ComponentException e) {
            LOG.warnf("Cannot drop replication slot %s on %s: %s", slotName(source.name()), url, e.getMessage());
        }
    }

    private String port(Map<String, OptionValue> options) {
        return options.containsKey("port") ? secrets.resolve(options, "port") : "5432";
    }

    @Override
    public List<Edge> lineage(Resource resource, Topology topology) {
        Resource.Source source = (Resource.Source) resource;
        return source.tables().stream()
                .map(t -> new Edge(Edge.postgres(source.connection(), t.toString()), Edge.kafka(source.topicFor(t)),
                        ResourceKind.SOURCE, source.name()))
                .toList();
    }

    private static String firstLine(String trace) {
        return trace.lines().findFirst().orElse(trace);
    }
}
