package org.streamhouseoss.controlplane.reconcile;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.streamhouseoss.controlplane.lineage.Edge;
import org.streamhouseoss.controlplane.state.StoredResource;
import org.streamhouseoss.model.Resource;
import org.streamhouseoss.model.ResourceKind;

/**
 * A snapshot of all declared (non-deleted) resources, and the one place that knows which topics
 * each of them writes ({@link #producedBy}) and reads ({@link #readBy}).
 */
public final class Topology {

    private static final Pattern TARGET = Pattern.compile(
            "^\\s*(?:CREATE\\s+(?:OR\\s+REPLACE\\s+)?TABLE(?:\\s+IF\\s+NOT\\s+EXISTS)?|INSERT\\s+(?:INTO|OVERWRITE))\\s+((?:`[^`]+`|[\\w$]+)(?:\\s*\\.\\s*(?:`[^`]+`|[\\w$]+))*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern NAME_PART = Pattern.compile("`([^`]+)`|([\\w$]+)");

    private final List<StoredResource> resources;
    private final Set<String> producedTopics = new LinkedHashSet<>();
    private final List<Pattern> topicPatterns = new ArrayList<>();

    private Topology(List<StoredResource> resources) {
        this.resources = resources;
        resources.forEach(r -> producedTopics.addAll(producedBy(r.resource())));
        producedTopics.forEach(t -> topicPatterns.add(Pattern.compile("`" + Pattern.quote(t) + "`|(?<![\\w.`])" + Pattern.quote(t) + "(?![\\w`])")));
    }

    public static Topology of(List<StoredResource> all) {
        return new Topology(all.stream().filter(r -> !r.deleted()).toList());
    }

    public List<StoredResource> resources() {
        return resources;
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

    /** Topics written by sources, materialized views and statements, in declaration order. */
    public Set<String> producedTopics() {
        return producedTopics;
    }

    /** The topics a resource writes. */
    public static List<String> producedBy(Resource resource) {
        return switch (resource) {
            case Resource.Source s -> s.tables().stream().map(s::topicFor).toList();
            case Resource.MaterializedView mv -> List.of(mv.name());
            case Resource.Statement st -> target(st.sql()).stream().toList();
            default -> List.of();
        };
    }

    /** The known topics a resource reads. */
    public List<String> readBy(Resource resource) {
        return switch (resource) {
            case Resource.MaterializedView mv -> referencedTopics(mv.query(), mv.name());
            case Resource.Statement st -> referencedTopics(st.sql(), target(st.sql()).orElse(null));
            case Resource.IcebergTable it -> List.of(it.name());
            case Resource.ContextTable ct -> List.of(ct.name());
            default -> List.of();
        };
    }

    /** Which of the known topics a Flink SQL query reads, by name (backquoted or bare). */
    public List<String> referencedTopics(String query, String excluding) {
        List<String> topics = new ArrayList<>();
        int i = 0;
        for (String topic : producedTopics) {
            if (!topic.equals(excluding) && topicPatterns.get(i).matcher(query).find()) {
                topics.add(topic);
            }
            i++;
        }
        return topics;
    }

    /** Resources that read the given topic. */
    public List<StoredResource> consumersOf(String topic) {
        return resources.stream().filter(r -> readBy(r.resource()).contains(topic)).toList();
    }

    /** Topic-to-topic edges of a resource that runs Flink SQL. */
    public List<Edge> lineage(Resource resource) {
        List<Edge> edges = new ArrayList<>();
        for (String output : producedBy(resource)) {
            for (String input : readBy(resource)) {
                edges.add(new Edge(Edge.kafka(input), Edge.kafka(output), resource.kind(), resource.name()));
            }
        }
        return edges;
    }

    /** The table a statement writes to, unqualified: CREATE TABLE x ... / INSERT INTO x ... */
    public static Optional<String> target(String sql) {
        Matcher m = TARGET.matcher(sql);
        if (!m.find()) {
            return Optional.empty();
        }
        String last = null;
        Matcher part = NAME_PART.matcher(m.group(1));
        while (part.find()) {
            last = part.group(1) != null ? part.group(1) : part.group(2);
        }
        return Optional.ofNullable(last);
    }
}
