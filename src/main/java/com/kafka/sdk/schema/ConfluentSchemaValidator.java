package com.kafka.sdk.schema;

import com.kafka.sdk.config.SchemaRegistryConfig;
import com.kafka.sdk.model.exception.SchemaValidationException;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema validator backed by Confluent Schema Registry.
 * Uses subject naming convention {@code <topic>-value}.
 * Caches schemas locally to avoid repeated registry calls.
 */
public class ConfluentSchemaValidator implements SchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(ConfluentSchemaValidator.class);

    private final SchemaRegistryClient srClient;
    private final SchemaCache cache;

    public ConfluentSchemaValidator(SchemaRegistryClient srClient, SchemaRegistryConfig config) {
        this.srClient = srClient;
        this.cache = new SchemaCache(config.getCacheMaxSize(), config.getCacheTtlSeconds());
    }

    @Override
    public void validate(String topic, Object payload) throws SchemaValidationException {
        if (payload == null) {
            throw new SchemaValidationException("Payload must not be null for topic: " + topic);
        }

        String subject = topic + "-value";

        Schema avroSchema = resolveSchema(topic, subject);

        if (payload instanceof GenericRecord) {
            validateGenericRecord((GenericRecord) payload, avroSchema, topic);
        }
        // For byte[] or String payloads, schema is fetched to confirm subject exists.
        // Deep Avro validation applies only to GenericRecord payloads in MVP.
    }

    private Schema resolveSchema(String topic, String subject) throws SchemaValidationException {
        Object cached = cache.get(subject);
        if (cached instanceof Schema) {
            return (Schema) cached;
        }

        try {
            SchemaMetadata metadata = srClient.getLatestSchemaMetadata(subject);
            Schema schema = new Schema.Parser().parse(metadata.getSchema());
            cache.put(subject, schema);
            log.debug("Schema fetched from registry: subject={} version={}", subject, metadata.getVersion());
            return schema;
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                throw new SchemaValidationException(
                        "No schema registered for subject '" + subject + "' in Schema Registry", e);
            }
            throw new SchemaValidationException(
                    "Schema Registry error for topic " + topic + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw new SchemaValidationException(
                    "Failed to fetch schema for topic " + topic + ": " + e.getMessage(), e);
        }
    }

    private void validateGenericRecord(GenericRecord record, Schema expectedSchema, String topic)
            throws SchemaValidationException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(expectedSchema);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            writer.write(record, encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new SchemaValidationException(
                    "Avro validation failed for topic " + topic
                            + ": payload does not match schema: " + e.getMessage(),
                    e);
        }
    }

    /** Exposes the underlying cache for testing (hit/miss counts). */
    public SchemaCache getCache() {
        return cache;
    }
}
