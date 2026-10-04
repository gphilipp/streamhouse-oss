package org.streamhouseoss.controlplane.clients;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.eclipse.microprofile.rest.client.inject.RestClient;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.enterprise.context.ApplicationScoped;

/** Tracks and cancels the Flink jobs the control plane submits. */
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

    private final FlinkJobsApi api;

    public FlinkJobs(@RestClient FlinkJobsApi api) {
        this.api = api;
    }

    public List<Job> list() {
        List<Job> jobs = new ArrayList<>();
        for (JsonNode job : Http.ok(api.overview(), "listing Flink jobs").getEntity().path("jobs")) {
            jobs.add(new Job(job.path("jid").asText(), job.path("name").asText(), job.path("state").asText(),
                    job.path("start-time").asLong()));
        }
        jobs.sort(Comparator.comparingLong(Job::startTime));
        return jobs;
    }

    public void cancel(String jobId) {
        api.cancel(jobId, "cancel");
    }

    /** Root cause of a failed job, for status messages. */
    public String rootException(String jobId) {
        JsonNode body = api.exceptions(jobId, 1).getEntity();
        String trace = body == null ? null : body.path("rootException").asText(null);
        if (trace == null) {
            JsonNode history = body == null ? null : body.path("exceptionHistory").path("entries");
            trace = history == null || history.isEmpty() ? "job failed" : history.get(0).path("stacktrace").asText("job failed");
        }
        return FlinkGateway.rootCause(trace);
    }
}
