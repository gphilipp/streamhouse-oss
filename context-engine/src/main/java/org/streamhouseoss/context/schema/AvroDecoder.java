package org.streamhouseoss.context.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Decodes Kafka keys and values in the Confluent wire format: magic byte {@code 0}, a 4-byte
 * big-endian schema id, then Avro binary. Debezium (via the Apicurio converter in Confluent
 * mode) and Flink's {@code avro-confluent} format both produce it. Keys that are not in this
 * format are treated as UTF-8 strings.
 */
@ApplicationScoped
public class AvroDecoder {

    /** A decoded payload with the writer schema and its registry id. */
    public record Decoded(int schemaId, Schema schema, Object value) {
    }

    private static final byte MAGIC = 0;
    private static final int UNKNOWN_SCHEMA = -1;

    private final SchemaRegistryClient registry;
    private final Map<Integer, GenericDatumReader<Object>> readers = new ConcurrentHashMap<>();

    public AvroDecoder(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    public static boolean isWireFormat(byte[] bytes) {
        return bytes != null && bytes.length >= 5 && bytes[0] == MAGIC;
    }

    public Decoded decode(byte[] bytes) {
        if (!isWireFormat(bytes)) {
            throw new SchemaException("payload is not in the Confluent Avro wire format");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.get();
        int schemaId = buffer.getInt();
        Schema schema = registry.schemaById(schemaId);
        GenericDatumReader<Object> reader = readers.computeIfAbsent(schemaId, id -> new GenericDatumReader<>(schema));
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null);
        try {
            return new Decoded(schemaId, schema, reader.read(null, decoder));
        } catch (IOException | RuntimeException e) {
            throw new SchemaException("cannot decode Avro payload with schema id " + schemaId + ": " + e.getMessage(), e);
        }
    }

    /** Decodes a key: Avro when in wire format, otherwise its UTF-8 text. */
    public Decoded decodeKey(byte[] bytes) {
        if (isWireFormat(bytes)) {
            return decode(bytes);
        }
        String text = bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        return new Decoded(UNKNOWN_SCHEMA, Schema.create(Schema.Type.STRING), text);
    }
}
