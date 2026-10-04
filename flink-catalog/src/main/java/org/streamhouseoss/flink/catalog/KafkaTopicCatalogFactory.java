package org.streamhouseoss.flink.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * {@code CREATE CATALOG streamhouse WITH ('type' = 'streamhouse-kafka', 'bootstrap.servers' = ...,
 * 'schema-registry.url' = ..., 'default-database' = 'local')}. Options prefixed {@code properties.}
 * are passed to the Kafka clients (e.g. SASL settings).
 */
public class KafkaTopicCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "streamhouse-kafka";

    static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions.key("bootstrap.servers").stringType().noDefaultValue()
            .withDescription("Kafka bootstrap servers");
    static final ConfigOption<String> SCHEMA_REGISTRY_URL = ConfigOptions.key("schema-registry.url").stringType().noDefaultValue()
            .withDescription("Confluent-compatible schema registry API");
    static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions.key("default-database").stringType()
            .defaultValue("local").withDescription("Name of the single database, i.e. the Kafka cluster");
    static final ConfigOption<Integer> DEFAULT_PARTITIONS = ConfigOptions.key("default.partitions").intType().defaultValue(6)
            .withDescription("Partitions of topics created without DISTRIBUTED BY ... INTO n BUCKETS");
    static final ConfigOption<Integer> REPLICATION_FACTOR = ConfigOptions.key("default.replication-factor").intType()
            .defaultValue(-1).withDescription("Replication factor of created topics; -1 uses the broker default");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(DEFAULT_DATABASE, DEFAULT_PARTITIONS, REPLICATION_FACTOR);
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept("properties.");
        Map<String, String> kafkaProperties = new HashMap<>();
        context.getOptions().forEach((k, v) -> {
            if (k.startsWith("properties.")) {
                kafkaProperties.put(k.substring("properties.".length()), v);
            }
        });
        return new KafkaTopicCatalog(context.getName(), helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(BOOTSTRAP_SERVERS), helper.getOptions().get(SCHEMA_REGISTRY_URL), kafkaProperties,
                helper.getOptions().get(DEFAULT_PARTITIONS), helper.getOptions().get(REPLICATION_FACTOR).shortValue());
    }

}
