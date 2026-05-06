package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ClusterType;
import com.kafka.sdk.config.ConfigLoader;
import com.kafka.sdk.model.exception.ConfigurationException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigLoaderTest {

    @Test
    void fromEnvironment_throwsWhenRequiredEnvVarMissing() {
        // KAFKA_SDK_CLUSTER_TYPE not set → should fail
        assertThatThrownBy(ConfigLoader::fromEnvironment)
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void fromEnvironment_throwsForInvalidClusterType() {
        // Use a helper to test with env-like parsing logic
        // Since we can't set real env vars in tests easily, verify the parsing logic
        // by testing the error path with invalid input
        assertThatThrownBy(() -> {
            // Simulate invalid cluster type by calling fromFile with a temp file
            java.io.File tmp = java.io.File.createTempFile("sdk-config", ".properties");
            try {
                java.nio.file.Files.writeString(tmp.toPath(),
                        "kafka.sdk.cluster.type=INVALID\n"
                                + "kafka.sdk.bootstrap.servers=b:9092\n"
                                + "kafka.sdk.auth.mechanism=IAM\n"
                                + "kafka.sdk.schema.registry.url=http://sr:8081\n");
                ConfigLoader.fromFile(tmp.getAbsolutePath());
            } finally {
                tmp.delete();
            }
        }).isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("INVALID");
    }

    @Test
    void fromFile_parsesMskConfiguration() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("sdk-config-msk", ".properties");
        try {
            java.nio.file.Files.writeString(tmp.toPath(),
                    "kafka.sdk.cluster.name=msk-test\n"
                            + "kafka.sdk.cluster.type=MSK\n"
                            + "kafka.sdk.bootstrap.servers=b1:9098,b2:9098\n"
                            + "kafka.sdk.auth.mechanism=IAM\n"
                            + "kafka.sdk.schema.registry.url=http://sr:8081\n");

            ClusterConfig config = ConfigLoader.fromFile(tmp.getAbsolutePath());

            assertThat(config.getName()).isEqualTo("msk-test");
            assertThat(config.getType()).isEqualTo(ClusterType.MSK);
            assertThat(config.getBootstrapServers()).isEqualTo("b1:9098,b2:9098");
            assertThat(config.getSchemaRegistryConfig().getUrl()).isEqualTo("http://sr:8081");
        } finally {
            tmp.delete();
        }
    }

    @Test
    void fromFile_parsesStrimziScramConfiguration() throws Exception {
        java.io.File tmp = java.io.File.createTempFile("sdk-config-scram", ".properties");
        try {
            java.nio.file.Files.writeString(tmp.toPath(),
                    "kafka.sdk.cluster.type=STRIMZI_SCRAM\n"
                            + "kafka.sdk.bootstrap.servers=kafka:9094\n"
                            + "kafka.sdk.auth.mechanism=SCRAM_SHA_512\n"
                            + "kafka.sdk.scram.username=alice\n"
                            + "kafka.sdk.scram.password=secret\n"
                            + "kafka.sdk.schema.registry.url=http://sr:8081\n");

            ClusterConfig config = ConfigLoader.fromFile(tmp.getAbsolutePath());

            assertThat(config.getType()).isEqualTo(ClusterType.STRIMZI_SCRAM);
            assertThat(config.getAuthConfig().getScramUsername()).isEqualTo("alice");
        } finally {
            tmp.delete();
        }
    }
}
