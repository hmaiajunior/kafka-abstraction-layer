package com.kafka.sdk.producer;

import com.kafka.sdk.auth.AuthProvider;
import com.kafka.sdk.auth.MskIamAuthProvider;
import com.kafka.sdk.auth.MtlsAuthProvider;
import com.kafka.sdk.auth.ScramAuthProvider;
import com.kafka.sdk.config.ClusterConfig;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Creates configured {@link KafkaProducer} instances for a given {@link ClusterConfig}. */
public class KafkaClientFactory {

    public KafkaProducer<String, byte[]> create(ClusterConfig config) {
        Properties props = baseProperties(config);
        AuthProvider authProvider = resolveAuthProvider(config);
        props.putAll(authProvider.toKafkaProperties());
        return new KafkaProducer<>(props);
    }

    private Properties baseProperties(ClusterConfig config) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "kafka-producer-sdk-" + config.getName());
        return props;
    }

    private AuthProvider resolveAuthProvider(ClusterConfig config) {
        return switch (config.getType()) {
            case MSK -> new MskIamAuthProvider();
            case STRIMZI_MTLS -> new MtlsAuthProvider(config.getAuthConfig());
            case STRIMZI_SCRAM -> new ScramAuthProvider(config.getAuthConfig());
        };
    }
}
