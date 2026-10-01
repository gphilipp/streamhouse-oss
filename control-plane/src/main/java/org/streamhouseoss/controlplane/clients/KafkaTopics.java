package org.streamhouseoss.controlplane.clients;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.streamhouseoss.controlplane.StreamhouseConfig;

import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/** Topic administration. The platform runs with topic auto-creation off, so every topic is created here. */
@ApplicationScoped
public class KafkaTopics {

    private final Admin admin;
    private final StreamhouseConfig config;

    public KafkaTopics(@ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers, StreamhouseConfig config) {
        this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15000));
        this.config = config;
    }

    void onStop(@Observes ShutdownEvent event) {
        admin.close(Duration.ofSeconds(5));
    }

    public Set<String> list() {
        return get(admin.listTopics().names());
    }

    public boolean exists(String topic) {
        return list().contains(topic);
    }

    /** Whether the topic keeps only the latest record per key (cleanup.policy contains compact). */
    public boolean compacted(String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        String policy = get(admin.describeConfigs(List.of(resource)).all()).get(resource)
                .get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
        return policy != null && policy.contains(TopicConfig.CLEANUP_POLICY_COMPACT);
    }

    /** Creates a topic if it does not exist yet. Returns true when it was created. */
    public boolean ensure(String topic, boolean compacted) {
        NewTopic newTopic = new NewTopic(topic, Optional.of(config.topics().partitions()),
                Optional.of(config.topics().replicationFactor()))
                .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG,
                        compacted ? TopicConfig.CLEANUP_POLICY_COMPACT : TopicConfig.CLEANUP_POLICY_DELETE));
        try {
            admin.createTopics(List.of(newTopic)).all().get(30, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                return false;
            }
            throw new ComponentException("cannot create topic " + topic + ": " + e.getCause().getMessage(), e);
        } catch (TimeoutException | InterruptedException e) {
            throw new ComponentException("timed out creating topic " + topic, e);
        }
    }

    public void delete(Collection<String> topics) {
        try {
            admin.deleteTopics(topics).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw new ComponentException("cannot delete topics " + topics + ": " + e.getCause().getMessage(), e);
            }
        } catch (TimeoutException | InterruptedException e) {
            throw new ComponentException("timed out deleting topics " + topics, e);
        }
    }

    private static <T> T get(org.apache.kafka.common.KafkaFuture<T> future) {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new ComponentException("Kafka admin call failed: " + e.getCause().getMessage(), e);
        } catch (TimeoutException | InterruptedException e) {
            throw new ComponentException("Kafka admin call timed out", e);
        }
    }
}
