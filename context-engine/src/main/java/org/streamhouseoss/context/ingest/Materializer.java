package org.streamhouseoss.context.ingest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jboss.logging.Logger;
import org.streamhouseoss.context.schema.AvroColumns;
import org.streamhouseoss.context.schema.AvroDecoder;
import org.streamhouseoss.context.schema.Column;
import org.streamhouseoss.context.schema.SchemaException;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableInfo;
import org.streamhouseoss.context.store.TableStatus;
import org.streamhouseoss.model.TableMode;

/**
 * Materializes one topic into its serving table. Each poll's records are written in a single
 * transaction together with the next offsets, so the table is exactly-once with respect to the
 * topic: after a crash the consumer resumes from the offsets committed with the data.
 */
final class Materializer implements Runnable {

    private static final Logger LOG = Logger.getLogger(Materializer.class);
    private static final Duration LAG_REFRESH = Duration.ofSeconds(5);
    private static final int MAX_FLUSH_ATTEMPTS = 4;

    /** A pending change: an upsert/insert (values present) or a delete (values null). */
    private record Change(List<Object> key, Map<String, Object> values) {
    }

    private final String topic;
    private final TableMode mode;
    private final ServingStore store;
    private final AvroDecoder decoder;
    private final Properties consumerProps;
    private final Duration pollTimeout;

    private volatile boolean running = true;
    private volatile KafkaConsumer<byte[], byte[]> consumer;
    private final AtomicLong lag = new AtomicLong(-1);
    private final AtomicLong recordsIngested = new AtomicLong();

    /** Layout persisted in the serving table, and the layout needed by pending records. */
    private Layout committedLayout;
    private Layout layout;
    private final Map<String, Layout> layoutsBySchemaIds = new HashMap<>();

    Materializer(TableInfo table, ServingStore store, AvroDecoder decoder, String bootstrapServers,
            int maxRecords, Duration pollTimeout) {
        this.topic = table.topic();
        this.mode = table.mode();
        this.store = store;
        this.decoder = decoder;
        this.pollTimeout = pollTimeout;
        this.committedLayout = new Layout(table.keyColumns(), table.columns());
        this.layout = committedLayout;
        this.consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "context-engine-" + topic);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxRecords));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    }

    String topic() {
        return topic;
    }

    /** Records not yet materialized (end offsets minus position), or -1 when unknown. */
    long lag() {
        return lag.get();
    }

    long recordsIngested() {
        return recordsIngested.get();
    }

    void stop() {
        running = false;
        KafkaConsumer<byte[], byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }

    @Override
    public void run() {
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps)) {
            consumer = c;
            assignPartitions(c);
            Instant nextLagRefresh = Instant.now();
            while (running) {
                ConsumerRecords<byte[], byte[]> records = c.poll(pollTimeout);
                if (!records.isEmpty()) {
                    flush(records);
                }
                if (Instant.now().isAfter(nextLagRefresh)) {
                    assignPartitions(c);
                    refreshLag(c);
                    nextLagRefresh = Instant.now().plus(LAG_REFRESH);
                }
            }
        } catch (WakeupException e) {
            if (running) {
                fail(e);
            }
        } catch (RuntimeException e) {
            fail(e);
        } finally {
            consumer = null;
        }
    }

    private void fail(Exception e) {
        LOG.errorf(e, "Materialization of %s failed", topic);
        running = false;
        try {
            store.setStatus(topic, TableStatus.FAILED, e.getMessage());
        } catch (RuntimeException statusError) {
            LOG.errorf(statusError, "Cannot record failure of %s", topic);
        }
    }

    /** Assigns all partitions (also picking up partitions added later) and seeks to stored offsets. */
    private void assignPartitions(KafkaConsumer<byte[], byte[]> c) {
        List<PartitionInfo> infos = c.partitionsFor(topic, Duration.ofSeconds(10));
        if (infos == null || infos.isEmpty()) {
            throw new IllegalStateException("topic " + topic + " does not exist");
        }
        List<TopicPartition> partitions = infos.stream().map(p -> new TopicPartition(topic, p.partition())).toList();
        if (c.assignment().containsAll(partitions)) {
            return;
        }
        List<TopicPartition> added = partitions.stream().filter(p -> !c.assignment().contains(p)).toList();
        c.assign(partitions);
        Map<Integer, Long> offsets = store.offsets(topic);
        List<TopicPartition> fromStart = new ArrayList<>();
        for (TopicPartition p : added) {
            Long next = offsets.get(p.partition());
            if (next == null) {
                fromStart.add(p);
            } else {
                c.seek(p, next);
            }
        }
        if (!fromStart.isEmpty()) {
            c.seekToBeginning(fromStart);
        }
        LOG.infof("Materializing %s: %d partition(s), resuming from %s", topic, partitions.size(), offsets);
    }

    private void refreshLag(KafkaConsumer<byte[], byte[]> c) {
        try {
            Map<TopicPartition, Long> ends = c.endOffsets(c.assignment(), Duration.ofSeconds(5));
            long total = 0;
            for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
                total += Math.max(0, e.getValue() - c.position(e.getKey(), Duration.ofSeconds(5)));
            }
            lag.set(total);
        } catch (RuntimeException e) {
            LOG.debugf(e, "Cannot compute lag of %s", topic);
        }
    }

    private void flush(ConsumerRecords<byte[], byte[]> records) {
        // Decode first so schema problems fail fast without touching the database.
        Map<List<Object>, Change> upserts = new LinkedHashMap<>();
        List<Change> inserts = new ArrayList<>();
        Map<Integer, Long> nextOffsets = new HashMap<>();
        long maxTimestamp = Long.MIN_VALUE;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            nextOffsets.merge(record.partition(), record.offset() + 1, Math::max);
            maxTimestamp = Math.max(maxTimestamp, record.timestamp());
            Change change = decode(record);
            if (change == null) {
                continue;
            }
            if (mode == TableMode.UPSERT) {
                upserts.put(change.key(), change); // last change per key wins
            } else {
                inserts.add(change);
            }
        }
        OffsetDateTime lastRecordTs = OffsetDateTime.ofInstant(Instant.ofEpochMilli(maxTimestamp), ZoneOffset.UTC);
        List<Change> changes = mode == TableMode.UPSERT ? new ArrayList<>(upserts.values()) : inserts;

        for (int attempt = 1; ; attempt++) {
            try {
                write(changes, nextOffsets, lastRecordTs);
                recordsIngested.addAndGet(records.count());
                return;
            } catch (SQLException e) {
                if (attempt >= MAX_FLUSH_ATTEMPTS || !running) {
                    throw new IllegalStateException("cannot write to the serving store: " + e.getMessage(), e);
                }
                LOG.warnf("Write of %s failed (attempt %d), retrying: %s", topic, attempt, e.getMessage());
                sleep(Duration.ofMillis(500L << attempt));
            }
        }
    }

    private Change decode(ConsumerRecord<byte[], byte[]> record) {
        if (record.value() == null) {
            if (mode == TableMode.APPEND || committedLayout.columns().isEmpty() && layout.columns().isEmpty()) {
                return null; // nothing to delete
            }
            AvroDecoder.Decoded key = decoder.decodeKey(record.key());
            return new Change(keyValues(key), null);
        }
        AvroDecoder.Decoded value = decoder.decode(record.value());
        AvroDecoder.Decoded key = mode == TableMode.UPSERT ? decoder.decodeKey(requireKey(record)) : null;

        String layoutId = (key == null ? "" : key.schemaId() + ":" + key.schema().getType()) + "/" + value.schemaId();
        Layout recordLayout = layoutsBySchemaIds.computeIfAbsent(layoutId,
                id -> Layout.of(mode, key == null ? null : key.schema(), value.schema()));
        layout = layout.merge(recordLayout);

        Map<String, Object> values = new HashMap<>();
        GenericRecord row = (GenericRecord) value.value();
        for (Schema.Field field : row.getSchema().getFields()) {
            values.put(field.name(), AvroColumns.toJdbc(field.schema(), row.get(field.pos())));
        }
        List<Object> keyValues = List.of();
        if (key != null) {
            keyValues = keyValues(key);
            for (int i = 0; i < keyValues.size(); i++) {
                values.put(layout.keyColumns().get(i), keyValues.get(i));
            }
        } else {
            values.put(Column.KAFKA_PARTITION.name(), record.partition());
            values.put(Column.KAFKA_OFFSET.name(), record.offset());
        }
        values.put(Column.KAFKA_TIMESTAMP.name(), OffsetDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneOffset.UTC));
        return new Change(keyValues, values);
    }

    private byte[] requireKey(ConsumerRecord<byte[], byte[]> record) {
        if (record.key() == null) {
            throw new SchemaException("record at " + record.partition() + "/" + record.offset()
                    + " has no key; upsert mode needs keyed records (use mode 'append' for unkeyed topics)");
        }
        return record.key();
    }

    private static List<Object> keyValues(AvroDecoder.Decoded key) {
        if (key.value() instanceof GenericRecord record) {
            List<Object> values = new ArrayList<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                values.add(AvroColumns.toJdbc(field.schema(), record.get(field.pos())));
            }
            return values;
        }
        List<Object> values = new ArrayList<>();
        values.add(AvroColumns.toJdbc(key.schema(), key.value()));
        return values;
    }

    private void write(List<Change> changes, Map<Integer, Long> nextOffsets, OffsetDateTime lastRecordTs) throws SQLException {
        try (Connection c = store.connection()) {
            c.setAutoCommit(false);
            try {
                if (!layout.equals(committedLayout)) {
                    store.applyLayout(c, topic, mode, layout.keyColumns(), committedLayout.columns(), layout.columns());
                }
                if (!changes.isEmpty()) {
                    writeChanges(c, changes);
                }
                store.saveProgress(c, topic, nextOffsets, lastRecordTs);
                c.commit();
                committedLayout = layout;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        }
    }

    private void writeChanges(Connection c, List<Change> changes) throws SQLException {
        List<Column> columns = layout.columns();
        String table = ServingStore.qualified(topic);
        String columnList = columns.stream().map(col -> ServingStore.quote(col.name())).collect(Collectors.joining(", "));
        String placeholders = columns.stream().map(col -> col.type().placeholder()).collect(Collectors.joining(", "));
        String insert = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + placeholders + ")";
        if (mode == TableMode.UPSERT) {
            String keys = layout.keyColumns().stream().map(ServingStore::quote).collect(Collectors.joining(", "));
            String updates = columns.stream()
                    .filter(col -> !layout.keyColumns().contains(col.name()))
                    .map(col -> ServingStore.quote(col.name()) + " = EXCLUDED." + ServingStore.quote(col.name()))
                    .collect(Collectors.joining(", "));
            insert += " ON CONFLICT (" + keys + ") DO UPDATE SET " + updates;
        } else {
            insert += " ON CONFLICT DO NOTHING";
        }
        String delete = "DELETE FROM " + table + " WHERE " + layout.keyColumns().stream()
                .map(k -> ServingStore.quote(k) + " = ?").collect(Collectors.joining(" AND "));

        try (PreparedStatement upsert = c.prepareStatement(insert);
                PreparedStatement del = mode == TableMode.UPSERT ? c.prepareStatement(delete) : null) {
            boolean anyUpsert = false;
            boolean anyDelete = false;
            for (Change change : changes) {
                if (change.values() == null) {
                    for (int i = 0; i < change.key().size(); i++) {
                        del.setObject(i + 1, change.key().get(i));
                    }
                    del.addBatch();
                    anyDelete = true;
                } else {
                    for (int i = 0; i < columns.size(); i++) {
                        upsert.setObject(i + 1, change.values().get(columns.get(i).name()));
                    }
                    upsert.addBatch();
                    anyUpsert = true;
                }
            }
            if (anyDelete) {
                del.executeBatch();
            }
            if (anyUpsert) {
                upsert.executeBatch();
            }
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
