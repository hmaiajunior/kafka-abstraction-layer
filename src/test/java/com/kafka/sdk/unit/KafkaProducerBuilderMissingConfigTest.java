package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafka.sdk.KafkaProducerBuilder;
import com.kafka.sdk.model.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

class KafkaProducerBuilderMissingConfigTest {

    @Test
    void build_throwsConfigurationExceptionWhenNoClusterConfigSet() {
        assertThatThrownBy(() -> new KafkaProducerBuilder().build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("clusterConfig");
    }
}
