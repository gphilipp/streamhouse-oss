package org.streamhouseoss.controlplane.lineage;

import org.streamhouseoss.model.ResourceKind;

/**
 * Data flows from {@code source} to {@code target} through a resource. Datasets are URIs:
 * {@code postgres://<connection>/<schema>.<table>}, {@code kafka://<topic>},
 * {@code iceberg://lake/<namespace>.<table>}, {@code context://<table>}.
 */
public record Edge(String source, String target, ResourceKind viaKind, String viaName) {

    public static String kafka(String topic) {
        return "kafka://" + topic;
    }

    public static String postgres(String connection, String table) {
        return "postgres://" + connection + "/" + table;
    }

    public static String iceberg(String namespace, String table) {
        return "iceberg://lake/" + namespace + "." + table;
    }

    public static String context(String table) {
        return "context://" + table;
    }
}
