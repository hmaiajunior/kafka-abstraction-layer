package com.kafka.sdk.benchmark;

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
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH latency benchmark comparing SDK overhead vs direct KafkaProducer.send().
 *
 * Run via: java -jar target/benchmarks.jar LatencyBenchmark
 * Or via the main() method for IDE execution.
 *
 * Target: SDK p99 overhead ≤ 20ms vs direct send.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
public class LatencyBenchmark {

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "benchmark.bootstrap.servers", "localhost:9092");
    private static final String TOPIC = "benchmark-topic";

    private KafkaProducerImpl sdkProducer;
    private KafkaProducer<String, byte[]> directProducer;
    private Message sdkMessage;

    @Setup(Level.Trial)
    public void setup() {
        Properties props = buildProps();

        // SDK producer
        SchemaValidator mockValidator = mock(SchemaValidator.class);
        TraceEmitter traceEmitter = new TraceEmitter(OpenTelemetry.noop());
        MetricsEmitter metricsEmitter = new MetricsEmitter(new SimpleMeterRegistry(), "benchmark");
        ObservabilityContext obs = new ObservabilityContext(metricsEmitter, traceEmitter);
        KafkaProducer<String, byte[]> rawProducer = new KafkaProducer<>(props);
        sdkProducer = new KafkaProducerImpl(rawProducer, mockValidator, obs);

        // Direct producer (no SDK layer)
        directProducer = new KafkaProducer<>(buildProps());

        sdkMessage = Message.forTopic(TOPIC)
                .payload("benchmark-payload")
                .header("bench", "true")
                .build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        sdkProducer.close();
        directProducer.close();
    }

    @Benchmark
    public void sdkProduce(Blackhole bh) throws Exception {
        CompletableFuture<DeliveryResult> future = sdkProducer.produce(sdkMessage);
        bh.consume(future.get());
    }

    @Benchmark
    public void directProduce(Blackhole bh) throws Exception {
        org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> record =
                new org.apache.kafka.clients.producer.ProducerRecord<>(
                        TOPIC, null, "benchmark-payload".getBytes());
        CompletableFuture<Void> future = new CompletableFuture<>();
        directProducer.send(record, (metadata, ex) -> {
            if (ex == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(ex);
            }
        });
        bh.consume(future.get());
    }

    private static Properties buildProps() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0");
        return props;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(LatencyBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(10)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
