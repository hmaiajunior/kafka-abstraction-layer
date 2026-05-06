package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ClusterType;
import com.kafka.sdk.config.SchemaRegistryConfig;
import com.kafka.sdk.model.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

class ClusterConfigTest {

    private static AuthConfig iamAuth() {
        return AuthConfig.builder().mechanism(AuthMechanism.IAM).build();
    }

    private static SchemaRegistryConfig srConfig() {
        return SchemaRegistryConfig.builder().url("http://sr:8081").build();
    }

    @Test
    void build_throwsWhenTypeIsNull() {
        assertThatThrownBy(() -> ClusterConfig.builder()
                .bootstrapServers("broker:9098")
                .authConfig(iamAuth())
                .schemaRegistryConfig(srConfig())
                .build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("clusterType");
    }

    @Test
    void build_throwsWhenBootstrapServersIsNull() {
        assertThatThrownBy(() -> ClusterConfig.builder()
                .type(ClusterType.MSK)
                .authConfig(iamAuth())
                .schemaRegistryConfig(srConfig())
                .build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("bootstrapServers");
    }

    @Test
    void build_throwsWhenBootstrapServersIsBlank() {
        assertThatThrownBy(() -> ClusterConfig.builder()
                .type(ClusterType.MSK)
                .bootstrapServers("   ")
                .authConfig(iamAuth())
                .schemaRegistryConfig(srConfig())
                .build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("bootstrapServers");
    }

    @Test
    void build_throwsWhenAuthConfigIsNull() {
        assertThatThrownBy(() -> ClusterConfig.builder()
                .type(ClusterType.MSK)
                .bootstrapServers("broker:9098")
                .schemaRegistryConfig(srConfig())
                .build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("authConfig");
    }

    @Test
    void build_throwsWhenSchemaRegistryConfigIsNull() {
        assertThatThrownBy(() -> ClusterConfig.builder()
                .type(ClusterType.MSK)
                .bootstrapServers("broker:9098")
                .authConfig(iamAuth())
                .build())
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("schemaRegistryConfig");
    }

    @Test
    void build_succeedsWithAllRequiredFields() {
        ClusterConfig config = ClusterConfig.builder()
                .name("msk-prod")
                .type(ClusterType.MSK)
                .bootstrapServers("broker:9098")
                .authConfig(iamAuth())
                .schemaRegistryConfig(srConfig())
                .build();

        org.assertj.core.api.Assertions.assertThat(config.getName()).isEqualTo("msk-prod");
        org.assertj.core.api.Assertions.assertThat(config.getType()).isEqualTo(ClusterType.MSK);
    }
}
