package org.streamhouseoss.controlplane.reconcile;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.streamhouseoss.controlplane.clients.ComponentException;
import org.streamhouseoss.controlplane.clients.FlinkGateway;
import org.streamhouseoss.controlplane.clients.FlinkJobs;
import org.streamhouseoss.controlplane.flink.FlinkDdl;
import org.streamhouseoss.controlplane.state.StoredResource;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * The lifecycle shared by every resource that runs a Flink job. A job's name encodes what it runs
 * ({@code <prefix><hash>}), so a changed definition gets a new job and outdated ones are cancelled
 * by prefix.
 */
@ApplicationScoped
public class FlinkJobSupport {

    /** Submits the job in a prepared session (topic catalog current, job named). */
    @FunctionalInterface
    public interface Submission {
        Reconciler.Outcome submit(FlinkGateway.Session session);
    }

    private final FlinkJobs jobs;
    private final FlinkGateway gateway;
    private final FlinkDdl ddl;

    public FlinkJobSupport(FlinkJobs jobs, FlinkGateway gateway, FlinkDdl ddl) {
        this.jobs = jobs;
        this.gateway = gateway;
        this.ddl = ddl;
    }

    /**
     * Keeps the job {@code jobName} running: reports it when it runs, keeps a failed job failed
     * until the resource changes (or is re-applied), and otherwise submits it.
     */
    public Reconciler.Outcome run(StoredResource stored, String prefix, String jobName, Map<String, Object> details,
            Submission submission) {
        List<FlinkJobs.Job> all = jobs.list();
        all.stream().filter(j -> j.active() && j.name().startsWith(prefix) && !j.name().equals(jobName))
                .forEach(j -> jobs.cancel(j.id()));
        Optional<FlinkJobs.Job> job = all.stream().filter(j -> j.name().equals(jobName)).reduce((a, b) -> b);
        if (job.isPresent() && job.get().active()) {
            return Reconciler.Outcome.ready("job " + job.get().id() + " is " + job.get().state(), details);
        }
        boolean changed = stored.observedGeneration() < stored.generation();
        if (job.isPresent() && job.get().state().equals("FAILED") && !changed) {
            return Reconciler.Outcome.failed("job " + job.get().id() + " failed: " + jobs.rootException(job.get().id()));
        }
        try (FlinkGateway.Session session = gateway.open(jobName)) {
            ddl.useTopicCatalog().forEach(session::execute);
            // Rows whose key would be NULL are dropped rather than failing the job.
            session.execute(FlinkDdl.set("table.exec.sink.not-null-enforcer", "DROP"));
            session.execute(FlinkDdl.set("pipeline.name", jobName));
            return submission.submit(session);
        } catch (ComponentException e) {
            return Reconciler.Outcome.failed(e.getMessage());
        }
    }

    public void cancelAll(String prefix) {
        jobs.list().stream().filter(j -> j.active() && j.name().startsWith(prefix)).forEach(j -> jobs.cancel(j.id()));
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
