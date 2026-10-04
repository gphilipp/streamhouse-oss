package org.streamhouseoss.controlplane.reconcile;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.logging.Logger;
import org.streamhouseoss.controlplane.clients.Gravitino;
import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.lineage.OpenLineage;
import org.streamhouseoss.controlplane.state.DesiredState;
import org.streamhouseoss.controlplane.state.Phase;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.ResourceKind;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;

/**
 * Converges actual state to desired state. Runs periodically and right after every applied
 * statement. Resources are reconciled in dependency order (connections before sources before
 * views...) and deleted in reverse order. One pass at a time.
 */
@ApplicationScoped
public class ReconcileLoop {

    private static final Logger LOG = Logger.getLogger(ReconcileLoop.class);
    /** A resource that failed is retried after this long unless it is changed or re-applied. */
    static final Duration FAILURE_BACKOFF = Duration.ofSeconds(30);

    private static final List<ResourceKind> ORDER = List.of(ResourceKind.CONNECTION, ResourceKind.SOURCE,
            ResourceKind.MATERIALIZED_VIEW, ResourceKind.STATEMENT, ResourceKind.ICEBERG_TABLE, ResourceKind.CONTEXT_TABLE, ResourceKind.GRANT);

    private final DesiredState state;
    private final Gravitino gravitino;
    private final Map<ResourceKind, Reconciler> reconcilers = new EnumMap<>(ResourceKind.class);
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean rerun = new AtomicBoolean();
    private final ExecutorService triggers = Executors.newSingleThreadExecutor(r -> new Thread(r, "reconcile-trigger"));

    public ReconcileLoop(DesiredState state, Gravitino gravitino, Instance<Reconciler> all) {
        this.state = state;
        this.gravitino = gravitino;
        all.forEach(r -> reconcilers.put(r.kind(), r));
    }

    void onStop(@Observes ShutdownEvent event) {
        triggers.shutdownNow();
    }

    @Scheduled(every = "${streamhouse.reconcile-interval}", delayed = "2s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void scheduled() {
        runOnce();
    }

    /** Requests a pass soon without blocking the caller. */
    public void trigger() {
        triggers.submit(this::runOnce);
    }

    public void runOnce() {
        if (!lock.tryLock()) {
            rerun.set(true); // the running pass will loop once more
            return;
        }
        try {
            do {
                rerun.set(false);
                pass();
            } while (rerun.get());
        } finally {
            lock.unlock();
        }
    }

    private void pass() {
        List<StoredResource> all = state.list(null);
        Topology topology = Topology.of(all);
        Comparator<StoredResource> byDependency = Comparator.comparingInt(r -> ORDER.indexOf(r.kind()));
        // Deletions first, dependents before their dependencies.
        all.stream().filter(StoredResource::deleted).sorted(byDependency.reversed()).forEach(r -> delete(r, topology));
        all.stream().filter(r -> !r.deleted()).sorted(byDependency).forEach(r -> reconcile(r, topology));
    }

    private void reconcile(StoredResource stored, Topology topology) {
        boolean caughtUp = stored.observedGeneration() >= stored.generation();
        if (caughtUp && stored.phase() == Phase.FAILED && stored.statusUpdatedAt() != null
                && stored.statusUpdatedAt().isAfter(OffsetDateTime.now().minus(FAILURE_BACKOFF))) {
            return;
        }
        Reconciler reconciler = reconcilers.get(stored.kind());
        Reconciler.Outcome outcome;
        try {
            outcome = reconciler.reconcile(stored, topology);
        } catch (RuntimeException e) {
            LOG.warnf(e, "Reconciling %s %s failed", stored.kind(), stored.name());
            outcome = Reconciler.Outcome.failed(e.getMessage() == null ? e.toString() : e.getMessage());
        }
        // Failures are always written so their timestamp drives the retry backoff.
        boolean changed = outcome.phase() != stored.phase() || !caughtUp || outcome.phase() == Phase.FAILED
                || !java.util.Objects.equals(outcome.message(), stored.message());
        if (changed) {
            Map<String, Object> details = outcome.details().isEmpty() ? stored.details() : outcome.details();
            state.setStatus(stored.kind(), stored.name(), outcome.phase(), outcome.message(), stored.generation(), details);
            if (outcome.phase() != stored.phase()) {
                LOG.infof("%s %s: %s (%s)", stored.kind(), stored.name(), outcome.phase(), outcome.message());
            }
        }
        if (outcome.phase() == Phase.READY && (stored.phase() != Phase.READY || !caughtUp)) {
            List<Edge> edges = reconciler.lineage(stored.resource(), topology);
            state.replaceLineage(stored.kind(), stored.name(), edges);
            if (!edges.isEmpty()) {
                gravitino.emitLineage(OpenLineage.running(stored.kind().name().toLowerCase() + ":" + stored.name(), edges));
            }
        }
    }

    private void delete(StoredResource stored, Topology topology) {
        try {
            if (reconcilers.get(stored.kind()).delete(stored, topology)) {
                state.purge(stored.kind(), stored.name());
                LOG.infof("%s %s: deleted", stored.kind(), stored.name());
            }
        } catch (RuntimeException e) {
            LOG.warnf("Deleting %s %s failed, will retry: %s", stored.kind(), stored.name(), e.getMessage());
            state.setStatus(stored.kind(), stored.name(), Phase.DELETING, e.getMessage(), stored.generation(), stored.details());
        }
    }
}
