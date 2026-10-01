package org.streamhouseoss.controlplane.clients;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.StreamhouseConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

/** Flink JobManager REST API, used to track and cancel the jobs the control plane submits. */
@ApplicationScoped
public class FlinkJobs {

    public record Job(String id, String name, String state, long startTime) {
        public boolean active() {
            return switch (state) {
                case "RUNNING", "CREATED", "INITIALIZING", "RESTARTING", "RECONCILING" -> true;
                default -> false;
            };
        }
    }

    private final JsonHttp http;

    public FlinkJobs(StreamhouseConfig config, ObjectMapper mapper) {
        this.http = new JsonHttp(mapper, config.flink().jobmanagerUrl(), Map.of());
    }

    public List<Job> list() {
        List<Job> jobs = new ArrayList<>();
        for (JsonNode job : http.get("/jobs/overview").requireOk("listing Flink jobs").body().path("jobs")) {
            jobs.add(new Job(job.path("jid").asText(), job.path("name").asText(), job.path("state").asText(),
                    job.path("start-time").asLong()));
        }
        return jobs;
    }

    /** The most recently started job with this name. */
    public Optional<Job> latest(String name) {
        return list().stream().filter(j -> j.name().equals(name)).max(java.util.Comparator.comparingLong(Job::startTime));
    }

    public List<Job> activeWithPrefix(String prefix) {
        return list().stream().filter(j -> j.active() && j.name().startsWith(prefix)).toList();
    }

    public void cancel(String jobId) {
        http.patch("/jobs/" + jobId + "?mode=cancel");
    }

    /** Root cause of a failed job, for status messages. */
    public String rootException(String jobId) {
        JsonNode body = http.get("/jobs/" + jobId + "/exceptions?maxExceptions=1").body();
        String trace = body.path("rootException").asText(null);
        if (trace == null) {
            JsonNode history = body.path("exceptionHistory").path("entries");
            trace = history.isEmpty() ? "job failed" : history.get(0).path("stacktrace").asText("job failed");
        }
        return FlinkGateway.rootCause(trace);
    }
}
