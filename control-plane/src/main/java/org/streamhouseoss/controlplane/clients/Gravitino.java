package org.streamhouseoss.controlplane.clients;

import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestResponse;
import org.streamhouseoss.controlplane.StreamhouseConfig;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/** The metalake with its Kafka and Iceberg catalogs, and OpenLineage ingestion. */
@ApplicationScoped
public class Gravitino {

    private static final Logger LOG = Logger.getLogger(Gravitino.class);

    private final GravitinoApi api;
    private final String metalake;

    public Gravitino(@RestClient GravitinoApi api, StreamhouseConfig config) {
        this.api = api;
        this.metalake = config.metalake();
    }

    /** Creates the metalake and its catalogs if missing (409 = already there). */
    public void bootstrap(Map<String, Object> kafkaCatalog, Map<String, Object> icebergCatalog) {
        createIfMissing(api.createMetalake(Map.of("name", metalake, "comment", "Streamhouse OSS", "properties", Map.of())),
                "metalake " + metalake);
        createIfMissing(api.createCatalog(metalake, kafkaCatalog), "catalog " + kafkaCatalog.get("name"));
        createIfMissing(api.createCatalog(metalake, icebergCatalog), "catalog " + icebergCatalog.get("name"));
    }

    /** Sends an OpenLineage RunEvent. Lineage is best effort and never fails a reconcile. */
    public void emitLineage(Map<String, Object> runEvent) {
        try {
            Http.ok(api.lineage(runEvent), "sending lineage to Gravitino");
        } catch (RuntimeException e) {
            LOG.warnf("Cannot send lineage to Gravitino: %s", e.getMessage());
        }
    }

    private static void createIfMissing(RestResponse<JsonNode> response, String what) {
        if (response.getStatus() != 409) {
            Http.ok(response, "creating Gravitino " + what);
            LOG.infof("Created Gravitino %s", what);
        }
    }
}
