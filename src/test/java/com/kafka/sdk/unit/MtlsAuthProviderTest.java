package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.auth.MtlsAuthProvider;
import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class MtlsAuthProviderTest {

    @Test
    void toKafkaProperties_returnsCorrectSslProperties() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.MTLS)
                .tlsKeystorePath("/certs/client.jks")
                .tlsKeystorePassword("kpass".toCharArray())
                .tlsTruststorePath("/certs/ca.jks")
                .tlsTruststorePassword("tpass".toCharArray())
                .build();

        Properties props = new MtlsAuthProvider(auth).toKafkaProperties();

        assertThat(props.getProperty("security.protocol")).isEqualTo("SSL");
        assertThat(props.getProperty("ssl.keystore.location")).isEqualTo("/certs/client.jks");
        assertThat(props.getProperty("ssl.keystore.password")).isEqualTo("kpass");
        assertThat(props.getProperty("ssl.truststore.location")).isEqualTo("/certs/ca.jks");
        assertThat(props.getProperty("ssl.truststore.password")).isEqualTo("tpass");
    }

    @Test
    void toString_doesNotExposePasswords() {
        AuthConfig auth = AuthConfig.builder()
                .mechanism(AuthMechanism.MTLS)
                .tlsKeystorePath("/certs/client.jks")
                .tlsKeystorePassword("super-secret".toCharArray())
                .tlsTruststorePath("/certs/ca.jks")
                .build();

        String str = new MtlsAuthProvider(auth).toString();
        assertThat(str).doesNotContain("super-secret");
        assertThat(str).contains("MASKED");
    }
}
