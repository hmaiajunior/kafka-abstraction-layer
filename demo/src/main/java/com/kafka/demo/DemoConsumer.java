package com.kafka.demo;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plain kafka-clients consumer used only to observe messages landing on whichever cluster
 * is currently being targeted by the SDK-based producer. Does not depend on the SDK by design —
 * it represents an unrelated downstream system.
 */
public final class DemoConsumer {

    private static final Logger log = LoggerFactory.getLogger(DemoConsumer.class);

    private DemoConsumer() {}

    public static void main(String[] args) {
        String bootstrap = requireEnv("KAFKA_BOOTSTRAP_SERVERS");
        String topic = envOrDefault("DEMO_TOPIC", "demo-events");
        String groupId = envOrDefault("DEMO_GROUP_ID", "demo-consumer");
        String clusterName = envOrDefault("DEMO_CLUSTER_NAME", "unknown");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put("security.protocol", "SSL");
        props.put("ssl.keystore.location", requireEnv("KAFKA_SSL_KEYSTORE_LOCATION"));
        props.put("ssl.keystore.password", requireEnv("KAFKA_SSL_KEYSTORE_PASSWORD"));
        props.put("ssl.key.password",
                envOrDefault("KAFKA_SSL_KEY_PASSWORD", System.getenv("KAFKA_SSL_KEYSTORE_PASSWORD")));
        props.put("ssl.truststore.location", requireEnv("KAFKA_SSL_TRUSTSTORE_LOCATION"));
        props.put("ssl.truststore.password", requireEnv("KAFKA_SSL_TRUSTSTORE_PASSWORD"));
        props.put("ssl.endpoint.identification.algorithm", "");

        log.info("DemoConsumer starting: cluster={} bootstrap={} topic={} groupId={}",
                clusterName, bootstrap, topic, groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            while (true) {
                ConsumerRecords<String, String> records;
                try {
                    records = consumer.poll(Duration.ofMillis(1000));
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    break;
                }
                for (ConsumerRecord<String, String> r : records) {
                    Header h = r.headers().lastHeader("kafka.sdk.correlationId");
                    String corr = h != null ? new String(h.value(), StandardCharsets.UTF_8) : "-";
                    Header tc = r.headers().lastHeader("target-cluster");
                    String tcv = tc != null ? new String(tc.value(), StandardCharsets.UTF_8) : "-";
                    log.info("[{}] partition={} offset={} key={} target={} correlationId={} value={}",
                            clusterName, r.partition(), r.offset(), r.key(), tcv, corr, r.value());
                }
            }
        }
        log.info("DemoConsumer stopped.");
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Missing env var: " + name);
        }
        return v;
    }

    private static String envOrDefault(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }
}
