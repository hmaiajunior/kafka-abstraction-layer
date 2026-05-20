package com.kafka.demo;

import com.kafka.sdk.KafkaProducer;
import com.kafka.sdk.KafkaProducerBuilder;
import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ConfigLoader;
import com.kafka.sdk.model.Message;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continuously produces JSON messages via the Kafka SDK using configuration loaded entirely
 * from KAFKA_SDK_* environment variables. Switching the target cluster is a pure restart
 * with a different env var set — there is no code path that knows about MSK vs Strimzi.
 */
public final class DemoProducer {

    private static final Logger log = LoggerFactory.getLogger(DemoProducer.class);

    private DemoProducer() {}

    public static void main(String[] args) throws InterruptedException {
        String topic = envOrDefault("DEMO_TOPIC", "demo-events");
        long intervalMs = Long.parseLong(envOrDefault("DEMO_INTERVAL_MS", "1000"));
        String source = envOrDefault("DEMO_SOURCE", "demo-producer");

        ClusterConfig config = ConfigLoader.fromEnvironment();
        log.info("DemoProducer starting: cluster={} bootstrap={} topic={} intervalMs={}",
                config.getName(), config.getBootstrapServers(), topic, intervalMs);

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping producer loop.");
            shutdown.countDown();
        }));

        try (KafkaProducer producer = new KafkaProducerBuilder()
                .withClusterConfig(config)
                .build()) {

            long count = 0;
            while (shutdown.getCount() > 0) {
                String id = UUID.randomUUID().toString();
                long ts = Instant.now().toEpochMilli();
                String payload = String.format(
                        "{\"id\":\"%s\",\"timestamp\":%d,\"source\":\"%s\",\"payload\":\"msg-%d\"}",
                        id, ts, source, count);

                Message msg = Message.forTopic(topic)
                        .key(String.valueOf(count).getBytes(StandardCharsets.UTF_8))
                        .payload(payload)
                        .header("producer-instance", source)
                        .header("target-cluster", config.getName())
                        .build();

                final long seq = count;
                producer.produce(msg).whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.error("[{}] produce #{} threw: {}", config.getName(), seq, ex.toString());
                    } else if (res.isSuccess()) {
                        log.info("[{}] produced #{} → partition={} offset={} correlationId={}",
                                config.getName(), seq, res.getPartition(), res.getOffset(),
                                res.getCorrelationId());
                    } else {
                        log.warn("[{}] produce #{} failed: code={} msg={} correlationId={}",
                                config.getName(), seq, res.getErrorCode(), res.getErrorMessage(),
                                res.getCorrelationId());
                    }
                });

                count++;
                shutdown.await(intervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        }
        log.info("DemoProducer stopped.");
    }

    private static String envOrDefault(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }
}
