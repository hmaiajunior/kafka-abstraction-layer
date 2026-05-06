package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kafka.sdk.config.SchemaRegistryConfig;
import com.kafka.sdk.model.exception.SchemaValidationException;
import com.kafka.sdk.schema.ConfluentSchemaValidator;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConfluentSchemaValidatorTest {

    private static final String SCHEMA_JSON = """
            {"type":"record","name":"Order","fields":[
              {"name":"id","type":"string"},
              {"name":"amount","type":"double"}
            ]}
            """;

    @Mock
    private SchemaRegistryClient mockSrClient;

    private ConfluentSchemaValidator validator;

    @BeforeEach
    void setUp() {
        SchemaRegistryConfig config = SchemaRegistryConfig.builder()
                .url("http://sr:8081")
                .cacheMaxSize(10)
                .cacheTtlSeconds(60)
                .build();
        validator = new ConfluentSchemaValidator(mockSrClient, config);
    }

    @Test
    void validate_acceptsValidGenericRecord() throws Exception {
        when(mockSrClient.getLatestSchemaMetadata("orders-value"))
                .thenReturn(new SchemaMetadata(1, 1, SCHEMA_JSON));

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", "order-123");
        record.put("amount", 99.99);

        assertThatCode(() -> validator.validate("orders", record)).doesNotThrowAnyException();
    }

    @Test
    void validate_rejectsGenericRecordWithMissingRequiredField() throws Exception {
        String strictSchema = """
                {"type":"record","name":"Order","fields":[
                  {"name":"id","type":"string"},
                  {"name":"amount","type":"double"}
                ]}
                """;
        String looseSchema = """
                {"type":"record","name":"Order","fields":[
                  {"name":"id","type":"string"}
                ]}
                """;
        // Registry has strict schema, payload was built with loose schema
        when(mockSrClient.getLatestSchemaMetadata("orders-value"))
                .thenReturn(new SchemaMetadata(1, 1, strictSchema));

        Schema loose = new Schema.Parser().parse(looseSchema);
        GenericRecord record = new GenericData.Record(loose);
        record.put("id", "order-456");

        assertThatThrownBy(() -> validator.validate("orders", record))
                .isInstanceOf(SchemaValidationException.class);
    }

    @Test
    void validate_callsRegistryExactlyOnceForSameTopic() throws Exception {
        when(mockSrClient.getLatestSchemaMetadata("orders-value"))
                .thenReturn(new SchemaMetadata(1, 1, SCHEMA_JSON));

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        for (int i = 0; i < 5; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("id", "id-" + i);
            record.put("amount", (double) i);
            validator.validate("orders", record);
        }

        // Registry called exactly once — subsequent calls hit cache
        verify(mockSrClient, times(1)).getLatestSchemaMetadata("orders-value");
    }

    @Test
    void validate_throwsForUnregisteredSubject() throws Exception {
        when(mockSrClient.getLatestSchemaMetadata("unknown-value"))
                .thenThrow(new io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException(
                        "Subject not found", 404, 40401));

        assertThatThrownBy(() -> validator.validate("unknown", "payload"))
                .isInstanceOf(SchemaValidationException.class)
                .hasMessageContaining("No schema registered");
    }
}
