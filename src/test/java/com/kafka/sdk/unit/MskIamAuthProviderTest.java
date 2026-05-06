package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.auth.MskIamAuthProvider;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class MskIamAuthProviderTest {

    @Test
    void toKafkaProperties_returnsAllFourRequiredIamProperties() {
        MskIamAuthProvider provider = new MskIamAuthProvider();

        Properties props = provider.toKafkaProperties();

        assertThat(props.getProperty("security.protocol")).isEqualTo("SASL_SSL");
        assertThat(props.getProperty("sasl.mechanism")).isEqualTo("AWS_MSK_IAM");
        assertThat(props.getProperty("sasl.jaas.config"))
                .contains("IAMLoginModule");
        assertThat(props.getProperty("sasl.client.callback.handler.class"))
                .contains("IAMClientCallbackHandler");
    }

    @Test
    void toString_doesNotContainSensitiveData() {
        MskIamAuthProvider provider = new MskIamAuthProvider();
        assertThat(provider.toString()).doesNotContain("password", "secret", "token");
    }
}
