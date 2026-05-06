package com.kafka.sdk.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.kafka.sdk.model.DeliveryResult;
import com.kafka.sdk.model.Message;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.SchemaValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end migration test: produces to cluster-A, then switches to cluster-B using
 * a different KafkaProducerImpl instance — representing config-only cluster migration.
 * Application code (produce call) is IDENTICAL between both runs.
 */
@Testcontainers
class ClusterMigrationTest {

    @Container
    static final KafkaContainer clusterA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static final KafkaContainer clusterB = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    private static final String TOPIC = "migration-topic";
    private static final int MESSAGE_COUNT = 10;

    @BeforeAll
    static void createTopics() throws Exception {
        for (KafkaContainer cluster : List.of(clusterA, clusterB)) {
            try (AdminClient admin = AdminClient.create(
                    Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
            }
        }
    }

    @Test
    void produce_sameApplicationCodeWorksOnBothClusters() throws Exception {
        // === Phase 1: Produce to cluster A ===
        KafkaProducerImpl producerA = buildProducer(clusterA.getBootstrapServers());
        List<DeliveryResult> resultsA = produceMessages(producerA, MESSAGE_COUNT);
        producerA.close();

        assertThat(resultsA).allMatch(DeliveryResult::isSuccess);
        assertThat(resultsA).allMatch(r -> r.getOffset() >= 0);

        // === Phase 2: "Migrate" — swap bootstrap servers (config change only) ===
        // Application code (produceMessages) is IDENTICAL — only the producer instance changes
        KafkaProducerImpl producerB = buildProducer(clusterB.getBootstrapServers());
        List<DeliveryResult> resultsB = produceMessages(producerB, MESSAGE_COUNT);
        producerB.close();

        assertThat(resultsB).allMatch(DeliveryResult::isSuccess);
        assertThat(resultsB).allMatch(r -> r.getOffset() >= 0);
    }

    /** This method represents the application's produce logic — identical for both clusters. */
    private List<DeliveryResult> produceMessages(KafkaProducerImpl producer, int count)
            throws Exception {
        List<CompletableFuture<DeliveryResult>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Message msg = Message.forTopic(TOPIC)
                    .payload("event-" + i)
                    .header("sequence", String.valueOf(i))
                    .build();
            futures.add(producer.produce(msg));
        }
        List<DeliveryResult> results = new ArrayList<>();
        for (CompletableFuture<DeliveryResult> f : futures) {
            results.add(f.get());
        }
        return results;
    }

    private KafkaProducerImpl buildProducer(String bootstrapServers) {
        SchemaValidator mockValidator = mock(SchemaValidator.class);
        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "migration-test");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(props);
        return new KafkaProducerImpl(rawProducer, mockValidator, obs);
    }
}
