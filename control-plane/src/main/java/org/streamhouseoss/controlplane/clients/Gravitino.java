package org.streamhouseoss.controlplane.clients;

import java.util.Map;

import org.jboss.logging.Logger;
import org.streamhouseoss.controlplane.StreamhouseConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/** Gravitino REST API: the metalake with its Kafka and Iceberg catalogs, and OpenLineage ingestion. */
@ApplicationScoped
public class Gravitino {

    private static final Logger LOG = Logger.getLogger(Gravitino.class);

    private final JsonHttp http;
    private final StreamhouseConfig config;

    public Gravitino(StreamhouseConfig config, ObjectMapper mapper) {
        this.config = config;
        this.http = new JsonHttp(mapper, config.gravitinoUrl(), Map.of(
                "Accept", "application/vnd.gravitino.v1+json",
                "Content-Type", "application/json"));
    }

    /** Creates the metalake and its catalogs if missing (409 = already there). */
    public void bootstrap(Map<String, Object> kafkaCatalog, Map<String, Object> icebergCatalog) {
        createIfMissing("/api/metalakes", Map.of("name", config.metalake(), "comment", "Streamhouse OSS", "properties", Map.of()),
                "metalake " + config.metalake());
        String catalogs = "/api/metalakes/" + config.metalake() + "/catalogs";
        createIfMissing(catalogs, kafkaCatalog, "catalog " + kafkaCatalog.get("name"));
        createIfMissing(catalogs, icebergCatalog, "catalog " + icebergCatalog.get("name"));
    }

    /** Sends an OpenLineage RunEvent. Lineage is best effort and never fails a reconcile. */
    public void emitLineage(Map<String, Object> runEvent) {
        try {
            JsonHttp.Response response = http.post("/api/lineage", runEvent);
            if (!response.ok()) {
                LOG.warnf("Gravitino rejected a lineage event: HTTP %d %s", response.status(), JsonHttp.abbreviate(response.raw()));
            }
        } catch (ComponentException e) {
            LOG.warnf("Cannot send lineage to Gravitino: %s", e.getMessage());
        }
    }

    private void createIfMissing(String path, Map<String, Object> body, String what) {
        JsonHttp.Response response = http.post(path, body);
        if (response.status() != 409) {
            response.requireOk("creating Gravitino " + what);
            LOG.infof("Created Gravitino %s", what);
        }
    }
}
