package org.streamhouseoss.context.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.streamhouseoss.context.ContextConfig;
import org.streamhouseoss.context.store.ServingStore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/**
 * Records every query (allowed or not) in {@code serving._audit} and publishes it as JSON to the
 * audit topic so it can be retained, shipped to a SIEM, or itself materialized.
 */
@ApplicationScoped
public class AuditLog {

    private static final Logger LOG = Logger.getLogger(AuditLog.class);

    public enum Channel {
        REST, MCP
    }

    public enum Outcome {
        OK, DENIED, INVALID, ERROR
    }

    public record Entry(Channel channel, Access.Caller caller, String topic, String query, Outcome outcome,
            Integer rowCount, long elapsedMs, String error) {
    }

    private final ServingStore store;
    private final ObjectMapper mapper;
    private final String topic;
    private final KafkaProducer<String, String> producer;

    public AuditLog(ServingStore store, ObjectMapper mapper, ContextConfig config,
            @ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers) {
        this.store = store;
        this.mapper = mapper;
        this.topic = config.auditTopic();
        this.producer = topic.isBlank() ? null : new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, "context-engine-audit",
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000,
                ProducerConfig.LINGER_MS_CONFIG, 50),
                new StringSerializer(), new StringSerializer());
    }

    void onStop(@Observes ShutdownEvent event) {
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }

    public void record(Entry entry) {
        try (Connection c = store.connection();
                PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO serving._audit (principal, roles, channel, topic, query, outcome, row_count, elapsed_ms, error)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
            ps.setString(1, entry.caller().principal());
            ps.setArray(2, c.createArrayOf("text", entry.caller().roles().toArray()));
            ps.setString(3, entry.channel().name());
            ps.setString(4, entry.topic());
            ps.setString(5, entry.query());
            ps.setString(6, entry.outcome().name());
            ps.setObject(7, entry.rowCount());
            ps.setInt(8, (int) Math.min(Integer.MAX_VALUE, entry.elapsedMs()));
            ps.setString(9, entry.error());
            ps.executeUpdate();
        } catch (SQLException e) {
            LOG.errorf(e, "Cannot write audit entry %s", entry);
        }
        publish(entry);
    }

    private void publish(Entry entry) {
        if (producer == null) {
            return;
        }
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("at", OffsetDateTime.now().toString());
        event.put("service", "context-engine");
        event.put("principal", entry.caller().principal());
        event.put("roles", entry.caller().roles());
        event.put("channel", entry.channel());
        event.put("topic", entry.topic());
        event.put("query", entry.query());
        event.put("outcome", entry.outcome());
        event.put("rowCount", entry.rowCount());
        event.put("elapsedMs", entry.elapsedMs());
        Optional.ofNullable(entry.error()).ifPresent(e -> event.put("error", e));
        try {
            producer.send(new ProducerRecord<>(topic, entry.caller().principal(), mapper.writeValueAsString(event)), (md, e) -> {
                if (e != null) {
                    LOG.warnf("Cannot publish audit event to %s: %s", topic, e.getMessage());
                }
            });
        } catch (JsonProcessingException | RuntimeException e) {
            LOG.warnf("Cannot publish audit event to %s: %s", topic, e.getMessage());
        }
    }
}
