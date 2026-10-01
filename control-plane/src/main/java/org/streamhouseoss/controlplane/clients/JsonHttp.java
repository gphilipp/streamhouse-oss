package org.streamhouseoss.controlplane.clients;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;

/** Tiny JSON-over-HTTP helper shared by the component clients. */
public final class JsonHttp {

    public record Response(int status, JsonNode body, String raw) {
        public boolean ok() {
            return status >= 200 && status < 300;
        }

        public Response requireOk(String what) {
            if (!ok()) {
                throw new ComponentException(what + " failed with HTTP " + status + ": " + abbreviate(raw));
            }
            return this;
        }
    }

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper mapper;
    private final String baseUrl;
    private final Map<String, String> headers;

    public JsonHttp(ObjectMapper mapper, String baseUrl, Map<String, String> headers) {
        this.mapper = mapper;
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.headers = headers;
    }

    public Response get(String path) {
        return send("GET", path, null, Map.of());
    }

    public Response delete(String path) {
        return send("DELETE", path, null, Map.of());
    }

    public Response post(String path, Object body) {
        return send("POST", path, body, Map.of());
    }

    public Response put(String path, Object body) {
        return send("PUT", path, body, Map.of());
    }

    public Response patch(String path) {
        return send("PATCH", path, null, Map.of());
    }

    public Response send(String method, String path, Object body, Map<String, String> extraHeaders) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Accept", "application/json");
            headers.forEach(request::header);
            extraHeaders.forEach(request::header);
            if (body == null) {
                request.method(method, HttpRequest.BodyPublishers.noBody());
            } else {
                if (!headers.containsKey("Content-Type")) {
                    request.header("Content-Type", "application/json");
                }
                String json = body instanceof String s ? s : mapper.writeValueAsString(body);
                request.method(method, HttpRequest.BodyPublishers.ofString(json));
            }
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            JsonNode parsed = MissingNode.getInstance();
            if (response.body() != null && !response.body().isBlank()) {
                try {
                    parsed = mapper.readTree(response.body());
                } catch (IOException notJson) {
                    // keep raw body only
                }
            }
            return new Response(response.statusCode(), parsed, response.body());
        } catch (IOException e) {
            throw new ComponentException(method + " " + baseUrl + path + " failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ComponentException("interrupted calling " + baseUrl + path, e);
        }
    }

    static String abbreviate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() > 600 ? s.substring(0, 600) + "…" : s;
    }
}
