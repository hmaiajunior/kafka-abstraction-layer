package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.auth.ScramAuthProvider;
import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ScramAuthProviderTest {

    @Test
    void toKafkaProperties_returnsSaslSslWithScramMechanism() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.SCRAM_SHA_512)
                .scramUsername("alice")
                .scramPassword("s3cr3t".toCharArray())
                .build();

        Properties props = new ScramAuthProvider(auth).toKafkaProperties();

        assertThat(props.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(props.getProperty("sasl.mechanism")).isEqualTo("SCRAM-SHA-512");
        assertThat(props.getProperty("sasl.jaas.config"))
                .contains("ScramLoginModule")
                .contains("username=\"alice\"")
                .contains("password=\"s3cr3t\"");
    }

    @Test
    void toString_masksPassword() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.SCRAM_SHA_512)
                .scramUsername("alice")
                .scramPassword("s3cr3t".toCharArray())
                .build();

        String str = new ScramAuthProvider(auth).toString();
        assertThat(str).doesNotContain("s3cr3t");
        assertThat(str).contains("MASKED");
        assertThat(str).contains("alice");
    }
}
