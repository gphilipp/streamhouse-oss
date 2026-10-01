package org.streamhouseoss.flink.catalog;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * What the topic and its schemas cannot express about a table created through the catalog: the
 * key columns of raw keys, the key prefix, whether the value carries the key, the startup mode.
 * Stored in a compacted topic keyed by table name, so Kafka stays the only state.
 */
class TableDefinitions implements AutoCloseable {

    /** The parts of a {@link TableSpec} that are not derived from the registry or topic config. */
    record Definition(List<TableSpec.Column> keyColumns, String keyFormat, String keyFieldsPrefix, boolean valueIncludesKey,
            String valueFormat, String startupMode) {
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration CACHE = Duration.ofSeconds(2);

    private final String topic;
    private final Properties clientProperties;
    private final KafkaProducer<String, String> producer;
    private Map<String, Definition> cache = Map.of();
    private Instant cachedAt = Instant.EPOCH;

    TableDefinitions(String topic, Properties clientProperties) {
        this.topic = topic;
        this.clientProperties = clientProperties;
        Properties p = new Properties();
        p.putAll(clientProperties);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        this.producer = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
    }

    String topic() {
        return topic;
    }

    synchronized Optional<Definition> get(String table) {
        return Optional.ofNullable(all().get(table));
    }

    synchronized void put(String table, Definition definition) {
        send(table, encode(definition));
    }

    synchronized void remove(String table) {
        send(table, null);
    }

    private void send(String key, String value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value)).get(15, TimeUnit.SECONDS);
            cachedAt = Instant.EPOCH;
        } catch (Exception e) {
            throw new CatalogException("cannot write table definition " + key + " to " + topic + ": " + e.getMessage(), e);
        }
    }

    /** Reads the whole (small, compacted) topic; cached briefly. */
    private Map<String, Definition> all() {
        if (Instant.now().isBefore(cachedAt.plus(CACHE))) {
            return cache;
        }
        Properties p = new Properties();
        p.putAll(clientProperties);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "streamhouse-catalog-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Map<String, Definition> definitions = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(topic, Duration.ofSeconds(10)).forEach(i -> partitions.add(new TopicPartition(topic, i.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions, Duration.ofSeconds(10));
            Instant deadline = Instant.now().plusSeconds(15);
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp)) && Instant.now().isBefore(deadline)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.value() == null) {
                        definitions.remove(record.key());
                    } else {
                        definitions.put(record.key(), decode(record.value()));
                    }
                }
            }
        } catch (RuntimeException e) {
            throw new CatalogException("cannot read table definitions from " + topic + ": " + e.getMessage(), e);
        }
        cache = definitions;
        cachedAt = Instant.now();
        return definitions;
    }

    static String encode(Definition d) {
        ObjectNode node = JSON.createObjectNode();
        ArrayNode keys = node.putArray("keyColumns");
        for (TableSpec.Column c : d.keyColumns()) {
            keys.addObject().put("name", c.name()).put("type", c.type().getLogicalType().asSerializableString());
        }
        node.put("keyFormat", d.keyFormat());
        node.put("keyFieldsPrefix", d.keyFieldsPrefix());
        node.put("valueIncludesKey", d.valueIncludesKey());
        node.put("valueFormat", d.valueFormat());
        node.put("startupMode", d.startupMode());
        return node.toString();
    }

    static Definition decode(String json) {
        try {
            JsonNode node = JSON.readTree(json);
            List<TableSpec.Column> keys = new ArrayList<>();
            for (JsonNode k : node.path("keyColumns")) {
                keys.add(new TableSpec.Column(k.path("name").asText(), TypeConversions.fromLogicalToDataType(
                        LogicalTypeParser.parse(k.path("type").asText(), TableDefinitions.class.getClassLoader()))));
            }
            return new Definition(keys, node.path("keyFormat").isNull() ? null : node.path("keyFormat").asText(null),
                    node.path("keyFieldsPrefix").asText(""), node.path("valueIncludesKey").asBoolean(),
                    node.path("valueFormat").asText(TableSpec.AVRO), node.path("startupMode").asText(null));
        } catch (Exception e) {
            throw new CatalogException("corrupt table definition: " + json, e);
        }
    }

    @Override
    public void close() {
        producer.close(Duration.ofSeconds(5));
    }
}
