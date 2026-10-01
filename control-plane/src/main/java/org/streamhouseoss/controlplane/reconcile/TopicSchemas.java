package org.streamhouseoss.controlplane.reconcile;

import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.streamhouseoss.controlplane.clients.SchemaRegistry;
import org.streamhouseoss.controlplane.flink.FlinkDdl;

import jakarta.enterprise.context.ApplicationScoped;

/** The registered key and value schemas of a topic, as Flink columns. */
@ApplicationScoped
public class TopicSchemas {

    public record TopicSchema(List<FlinkDdl.FlinkColumn> columns, List<String> keyFields) {
    }

    private final SchemaRegistry registry;

    public TopicSchemas(SchemaRegistry registry) {
        this.registry = registry;
    }

    /** Empty until a producer (Debezium, a Flink job) has registered the value schema. */
    public Optional<TopicSchema> of(String topic) {
        Optional<Schema> value = registry.latest(topic + "-value");
        if (value.isEmpty()) {
            return Optional.empty();
        }
        List<String> keyFields = registry.latest(topic + "-key")
                .filter(k -> k.getType() == Schema.Type.RECORD)
                .map(k -> k.getFields().stream().map(Schema.Field::name).toList())
                .orElse(List.of());
        return Optional.of(new TopicSchema(FlinkDdl.columns(value.get()), keyFields));
    }
}
