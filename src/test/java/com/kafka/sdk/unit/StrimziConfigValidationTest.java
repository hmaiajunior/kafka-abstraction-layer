package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafka.sdk.KafkaProducerBuilder;
import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ClusterType;
import com.kafka.sdk.config.SchemaRegistryConfig;
import com.kafka.sdk.model.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

class StrimziConfigValidationTest {

    @Test
    void build_throwsWhenStrimziMtlsMissingKeystorePath() {
        ClusterConfig config = ClusterConfig.builder()
                .type(ClusterType.STRIMZI_MTLS)
                .bootstrapServers("kafka:9093")
                .authConfig(AuthConfig.builder()
                        .mechanism(AuthMechanism.MTLS)
                        // tlsKeystorePath intentionally omitted
                        .tlsTruststorePath("/certs/ca.jks")
                        .build())
                .schemaRegistryConfig(SchemaRegistryConfig.builder().url("http://sr:8081").build())
                .build();

        assertThatThrownBy(() -> new KafkaProducerBuilder().withClusterConfig(config).build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("tlsKeystorePath");
    }

    @Test
    void build_throwsWhenStrimziScramMissingUsername() {
        ClusterConfig config = ClusterConfig.builder()
                .type(ClusterType.STRIMZI_SCRAM)
                .bootstrapServers("kafka:9094")
                .authConfig(AuthConfig.builder()
                        .mechanism(AuthMechanism.SCRAM_SHA_512)
                        // scramUsername intentionally omitted
                        .scramPassword("pass".toCharArray())
                        .build())
                .schemaRegistryConfig(SchemaRegistryConfig.builder().url("http://sr:8081").build())
                .build();

        assertThatThrownBy(() -> new KafkaProducerBuilder().withClusterConfig(config).build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("scramUsername");
    }
}
