package org.streamhouseoss.controlplane.reconcile;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

/** A snapshot of all declared (non-deleted) resources, with the topics they produce. */
public record Topology(List<StoredResource> resources) {

    public static Topology of(List<StoredResource> all) {
        return new Topology(all.stream().filter(r -> !r.deleted()).toList());
    }

    public <T extends Resource> Stream<T> all(Class<T> type) {
        return resources.stream().map(StoredResource::resource).filter(type::isInstance).map(type::cast);
    }

    public Optional<StoredResource> find(ResourceKind kind, String name) {
        return resources.stream().filter(r -> r.kind() == kind && r.name().equals(name)).findFirst();
    }

    public Optional<Resource.Connection> connection(String name) {
        return all(Resource.Connection.class).filter(c -> c.name().equals(name)).findFirst();
    }

    /** Topics written by sources and materialized views, in declaration order. */
    public Set<String> producedTopics() {
        Set<String> topics = new LinkedHashSet<>();
        all(Resource.Source.class).forEach(s -> s.tables().forEach(t -> topics.add(s.topicFor(t))));
        all(Resource.MaterializedView.class).forEach(mv -> topics.add(mv.name()));
        return topics;
    }

    /** Which of the known topics a Flink SQL query reads, by name (backquoted or bare). */
    public List<String> referencedTopics(String query, String excluding) {
        return producedTopics().stream()
                .filter(t -> !t.equals(excluding))
                .filter(t -> query.contains("`" + t + "`")
                        || Pattern.compile("(?<![\\w.`])" + Pattern.quote(t) + "(?![\\w`])").matcher(query).find())
                .toList();
    }

    /** Resources that read the given topic. */
    public List<StoredResource> consumersOf(String topic) {
        return resources.stream().filter(r -> switch (r.resource()) {
            case Resource.MaterializedView mv -> referencedTopics(mv.query(), mv.name()).contains(topic);
            case Resource.IcebergTable it -> it.name().equals(topic);
            case Resource.ContextTable ct -> ct.name().equals(topic);
            default -> false;
        }).toList();
    }
}
