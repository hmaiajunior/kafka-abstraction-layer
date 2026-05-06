package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.auth.MskIamAuthProvider;
import com.kafka.sdk.auth.MtlsAuthProvider;
import com.kafka.sdk.auth.ScramAuthProvider;
import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Security audit: verifies that no credential values appear in provider toString() output.
 */
class CredentialLeakAuditTest {

    @Test
    void mskIamAuthProvider_toString_containsNoSensitiveData() {
        MskIamAuthProvider provider = new MskIamAuthProvider();
        String str = provider.toString();
        assertThat(str).doesNotContainIgnoringCase("password")
                .doesNotContainIgnoringCase("secret")
                .doesNotContainIgnoringCase("token");
    }

    @Test
    void mtlsAuthProvider_toString_masksKeystoreAndTruststorePasswords() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.MTLS)
                .tlsKeystorePath("/certs/client.jks")
                .tlsKeystorePassword("keystore-secret-pw".toCharArray())
                .tlsTruststorePath("/certs/ca.jks")
                .tlsTruststorePassword("truststore-secret-pw".toCharArray())
                .build();

        String str = new MtlsAuthProvider(auth).toString();
        assertThat(str).doesNotContain("keystore-secret-pw");
        assertThat(str).doesNotContain("truststore-secret-pw");
        assertThat(str).contains("MASKED");
    }

    @Test
    void scramAuthProvider_toString_masksPassword() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.SCRAM_SHA_512)
                .scramUsername("alice")
                .scramPassword("my-very-secret-password".toCharArray())
                .build();

        String str = new ScramAuthProvider(auth).toString();
        assertThat(str).doesNotContain("my-very-secret-password");
        assertThat(str).contains("MASKED");
    }

    @Test
    void authConfig_toString_masksAllPasswords() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.SCRAM_SHA_512)
                .scramUsername("alice")
                .scramPassword("scram-secret".toCharArray())
                .build();

        String str = auth.toString();
        assertThat(str).doesNotContain("scram-secret");
        assertThat(str).contains("MASKED");
    }

    @Test
    void scramAuthProvider_jaasConfig_containsPasswordInPropertiesButIsNotLogged() {
        // The JAAS config string contains the password as required by the Kafka client.
        // This test verifies the password IS in the properties (required for Kafka)
        // but is NOT in the toString() (which is what gets logged).
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.SCRAM_SHA_512)
                .scramUsername("alice")
                .scramPassword("runtime-secret".toCharArray())
                .build();

        ScramAuthProvider provider = new ScramAuthProvider(auth);
        Properties props = provider.toKafkaProperties();

        // Props contain the password (required by Kafka client) — this is correct
        assertThat(props.getProperty("sasl.jaas.config")).contains("runtime-secret");
        // But toString() does NOT expose it
        assertThat(provider.toString()).doesNotContain("runtime-secret");
    }
}
