package org.streamhouseoss.controlplane.lineage;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Builds OpenLineage RunEvents for the jobs the control plane runs (connectors, Flink jobs, materializers). */
public final class OpenLineage {

    private static final String PRODUCER = "https://github.com/streamhouse-oss/streamhouse";
    private static final String SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent";

    private OpenLineage() {
    }

    /** A RUNNING event linking a resource's inputs to its outputs. */
    public static Map<String, Object> running(String jobName, List<Edge> edges) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", "RUNNING");
        event.put("eventTime", OffsetDateTime.now().toString());
        event.put("run", Map.of("runId", UUID.randomUUID().toString()));
        event.put("job", Map.of("namespace", "streamhouse", "name", jobName));
        event.put("inputs", edges.stream().map(Edge::source).distinct().map(OpenLineage::dataset).toList());
        event.put("outputs", edges.stream().map(Edge::target).distinct().map(OpenLineage::dataset).toList());
        event.put("producer", PRODUCER);
        event.put("schemaURL", SCHEMA_URL);
        return event;
    }

    /** {@code kafka://orders} becomes namespace {@code kafka}, name {@code orders}. */
    static Map<String, Object> dataset(String uri) {
        int sep = uri.indexOf("://");
        return Map.of("namespace", uri.substring(0, sep), "name", uri.substring(sep + 3));
    }
}
