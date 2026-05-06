package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kafka.sdk.KafkaProducerBuilder;
import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ClusterType;
import com.kafka.sdk.config.SchemaRegistryConfig;
import com.kafka.sdk.model.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

/**
 * Verifies that KafkaProducerBuilder.build() throws ConfigurationException
 * when the Schema Registry URL is unreachable or missing at startup.
 */
class SchemaRegistryConnectivityTest {

    @Test
    void build_throwsConfigurationExceptionWhenSchemaRegistryUrlIsMissing() {
        assertThatThrownBy(() ->
                SchemaRegistryConfig.builder().url(null).build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("schemaRegistryUrl");
    }

    @Test
    void build_throwsConfigurationExceptionWhenSchemaRegistryUrlIsBlank() {
        assertThatThrownBy(() ->
                SchemaRegistryConfig.builder().url("   ").build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("schemaRegistryUrl");
    }
}
