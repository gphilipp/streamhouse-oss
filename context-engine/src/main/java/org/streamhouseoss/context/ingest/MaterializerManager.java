package org.streamhouseoss.context.ingest;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.streamhouseoss.context.ContextConfig;
import org.streamhouseoss.context.schema.AvroDecoder;
import org.streamhouseoss.context.store.ServingStore;
import org.streamhouseoss.context.store.TableInfo;
import org.streamhouseoss.context.store.TableStatus;
import org.streamhouseoss.model.TableMode;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/** Starts, stops and tracks one {@link Materializer} thread per enabled topic. */
@ApplicationScoped
public class MaterializerManager {

    private static final Logger LOG = Logger.getLogger(MaterializerManager.class);

    /** Raised when a topic cannot be enabled, e.g. because it does not exist. */
    public static class EnableException extends RuntimeException {
        public EnableException(String message) {
            super(message);
        }
    }

    private final ServingStore store;
    private final AvroDecoder decoder;
    private final ContextConfig config;
    private final MeterRegistry meters;
    private final String bootstrapServers;
    private final Map<String, Running> running = new ConcurrentHashMap<>();
    private Admin admin;

    private record Running(Materializer materializer, Thread thread) {
    }

    public MaterializerManager(ServingStore store, AvroDecoder decoder, ContextConfig config, MeterRegistry meters,
            @ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers) {
        this.store = store;
        this.decoder = decoder;
        this.config = config;
        this.meters = meters;
        this.bootstrapServers = bootstrapServers;
    }

    void onStart(@Observes StartupEvent event) {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
        createAuditTopic();
        for (TableInfo table : store.tables()) {
            if (table.status() != TableStatus.FAILED) {
                start(table);
            }
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        running.keySet().forEach(this::stop);
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
    }

    /** The audit topic must exist because brokers run with topic auto-creation disabled. */
    private void createAuditTopic() {
        if (config.auditTopic().isBlank()) {
            return;
        }
        NewTopic topic = new NewTopic(config.auditTopic(), Optional.empty(), Optional.empty())
                .configs(Map.of(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(30).toMillis())));
        try {
            admin.createTopics(java.util.List.of(topic)).all().get(10, TimeUnit.SECONDS);
            LOG.infof("Created audit topic %s", config.auditTopic());
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                LOG.warnf("Cannot create audit topic %s: %s", config.auditTopic(), e.getCause().getMessage());
            }
        } catch (TimeoutException e) {
            LOG.warnf("Timed out creating audit topic %s", config.auditTopic());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Enables (or re-enables) materialization of a topic. A {@code null} mode is inferred from the
     * topic's {@code cleanup.policy}: compacted topics are upserted, others appended.
     */
    public TableInfo enable(String topic, TableMode requestedMode, String description) {
        if (topic.length() > ServingStore.MAX_TABLE_NAME_LENGTH) {
            throw new EnableException("topic name is longer than " + ServingStore.MAX_TABLE_NAME_LENGTH + " characters");
        }
        TableMode mode = requestedMode != null ? requestedMode : inferMode(topic);
        stop(topic);
        TableInfo table = store.enable(topic, mode, description == null ? "" : description);
        start(table);
        return table;
    }

    public boolean disable(String topic) {
        stop(topic);
        return store.disable(topic);
    }

    public Optional<Long> lag(String topic) {
        return Optional.ofNullable(running.get(topic)).map(r -> r.materializer().lag()).filter(l -> l >= 0);
    }

    TableMode inferMode(String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try {
            Config topicConfig = admin.describeConfigs(java.util.List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource);
            String policy = topicConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
            return policy != null && policy.contains(TopicConfig.CLEANUP_POLICY_COMPACT) ? TableMode.UPSERT : TableMode.APPEND;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new EnableException("topic " + topic + " does not exist");
            }
            throw new EnableException("cannot describe topic " + topic + ": " + e.getCause().getMessage());
        } catch (TimeoutException e) {
            throw new EnableException("timed out describing topic " + topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EnableException("interrupted");
        }
    }

    private void start(TableInfo table) {
        Materializer materializer = new Materializer(table, store, decoder, bootstrapServers,
                config.batchMaxRecords(), config.batchMaxWait());
        Thread thread = Thread.ofPlatform().name("materializer-" + table.topic()).daemon().unstarted(materializer);
        running.put(table.topic(), new Running(materializer, thread));
        Gauge.builder("streamhouse.context.lag", materializer, Materializer::lag)
                .tag("topic", table.topic())
                .description("Records in the topic not yet materialized")
                .register(meters);
        Gauge.builder("streamhouse.context.records.ingested", materializer, Materializer::recordsIngested)
                .tag("topic", table.topic())
                .register(meters);
        thread.start();
    }

    private void stop(String topic) {
        Running r = running.remove(topic);
        if (r == null) {
            return;
        }
        r.materializer().stop();
        try {
            r.thread().join(Duration.ofSeconds(30));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        meters.find("streamhouse.context.lag").tag("topic", topic).meters().forEach(meters::remove);
        meters.find("streamhouse.context.records.ingested").tag("topic", topic).meters().forEach(meters::remove);
        LOG.infof("Stopped materializing %s", topic);
    }
}
