package org.streamhouseoss.controlplane.reconcile;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.clients.FlinkJobs;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Job bookkeeping shared by the reconcilers that run Flink jobs. A job's name encodes what it
 * runs ({@code <prefix><hash>}), so a changed definition gets a new job and stale ones are
 * cancelled by prefix.
 */
@ApplicationScoped
public class FlinkJobSupport {

    private final FlinkJobs jobs;

    public FlinkJobSupport(FlinkJobs jobs) {
        this.jobs = jobs;
    }

    public enum JobState {
        RUNNING, FAILED, ABSENT
    }

    public record Observed(JobState state, String jobId, String message) {
    }

    /** Cancels outdated jobs and reports the state of the current one. */
    public Observed observe(String prefix, String jobName) {
        jobs.activeWithPrefix(prefix).stream().filter(j -> !j.name().equals(jobName)).forEach(j -> jobs.cancel(j.id()));
        Optional<FlinkJobs.Job> job = jobs.latest(jobName);
        if (job.isPresent() && job.get().active()) {
            return new Observed(JobState.RUNNING, job.get().id(), "job " + job.get().id() + " is " + job.get().state());
        }
        if (job.isPresent() && job.get().state().equals("FAILED")) {
            return new Observed(JobState.FAILED, job.get().id(), "job " + job.get().id() + " failed: " + jobs.rootException(job.get().id()));
        }
        return new Observed(JobState.ABSENT, null, null);
    }

    public void cancelAll(String prefix) {
        jobs.activeWithPrefix(prefix).forEach(j -> jobs.cancel(j.id()));
    }

    public static Reconciler.Outcome running(Observed observed, Map<String, Object> details) {
        return Reconciler.Outcome.ready(observed.message(), details);
    }

    public static String hash(String... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : parts) {
                digest.update(part.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            return HexFormat.of().formatHex(digest.digest()).substring(0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
