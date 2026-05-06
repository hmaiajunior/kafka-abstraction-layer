package com.kafka.sdk.auth;

import java.util.Properties;

/**
 * Configures MSK IAM authentication via SASL/OAUTHBEARER.
 * Requires the {@code aws-msk-iam-auth} library on the classpath.
 * Credentials are sourced from the EC2 instance profile or IRSA role — never hardcoded.
 */
public class MskIamAuthProvider implements AuthProvider {

    @Override
    public Properties toKafkaProperties() {
        Properties props = new Properties();
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "AWS_MSK_IAM");
        props.setProperty(
                "sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.setProperty(
                "sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return props;
    }

    @Override
    public String toString() {
        return "MskIamAuthProvider{mechanism=AWS_MSK_IAM}";
    }
}
