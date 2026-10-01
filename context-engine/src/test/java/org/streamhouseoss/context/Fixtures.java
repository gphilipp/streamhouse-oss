package org.streamhouseoss.context;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Creates topics, registers schemas and produces Confluent-wire-format Avro records. */
final class Fixtures implements AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final Admin admin;
    private final KafkaProducer<byte[], byte[]> producer;

    Fixtures() {
        String bootstrap = StackResource.kafka.getBootstrapServers();
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
                new ByteArraySerializer(), new ByteArraySerializer());
    }

    void createTopic(String name, int partitions, boolean compacted) throws Exception {
        NewTopic topic = new NewTopic(name, Optional.of(partitions), Optional.empty())
                .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG,
                        compacted ? TopicConfig.CLEANUP_POLICY_COMPACT : TopicConfig.CLEANUP_POLICY_DELETE));
        admin.createTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
    }

    /** Registers a schema under a subject and returns its id. */
    int register(String subject, Schema schema) throws Exception {
        String body = JSON.writeValueAsString(Map.of("schema", schema.toString()));
        HttpRequest request = HttpRequest.newBuilder(URI.create(StackResource.registryUrl() + "/subjects/" + subject + "/versions"))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("register " + subject + ": " + response.statusCode() + " " + response.body());
        }
        return JSON.readTree(response.body()).get("id").asInt();
    }

    void send(String topic, byte[] key, byte[] value) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
    }

    static byte[] wire(int schemaId, Schema schema, Object datum) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<>(schema).write(datum, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] wire(int schemaId, GenericRecord record) {
        return wire(schemaId, record.getSchema(), record);
    }

    @Override
    public void close() {
        producer.close();
        admin.close();
    }
}
