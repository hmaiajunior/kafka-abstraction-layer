package com.kafka.sdk;

import com.kafka.sdk.config.AuthConfig;
import com.kafka.sdk.config.AuthMechanism;
import com.kafka.sdk.config.ClusterConfig;
import com.kafka.sdk.config.ClusterType;
import com.kafka.sdk.model.exception.ConfigurationException;
import com.kafka.sdk.observability.MetricsEmitter;
import com.kafka.sdk.observability.ObservabilityContext;
import com.kafka.sdk.observability.TraceEmitter;
import com.kafka.sdk.producer.KafkaClientFactory;
import com.kafka.sdk.producer.KafkaProducerImpl;
import com.kafka.sdk.schema.ConfluentSchemaValidator;
import com.kafka.sdk.schema.SchemaValidator;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluent builder that constructs a fully-wired {@link KafkaProducer}.
 * Validates all configuration and verifies Schema Registry connectivity at {@link #build()} time.
 * Throws {@link ConfigurationException} on any configuration error — fail-fast.
 */
public final class KafkaProducerBuilder {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerBuilder.class);

    private ClusterConfig clusterConfig;
    private MeterRegistry meterRegistry;
    private OpenTelemetry openTelemetry;

    /**
     * Sets the cluster configuration describing the target Kafka cluster and authentication.
     *
     * @param config the cluster configuration; must not be null
     * @return this builder
     */
    public KafkaProducerBuilder withClusterConfig(ClusterConfig config) {
        this.clusterConfig = config;
        return this;
    }

    /**
     * Sets a custom Micrometer {@link MeterRegistry} for emitting SDK metrics.
     * Defaults to {@link SimpleMeterRegistry} if not provided.
     *
     * @param registry the meter registry to use
     * @return this builder
     */
    public KafkaProducerBuilder withMeterRegistry(MeterRegistry registry) {
        this.meterRegistry = registry;
        return this;
    }

    /**
     * Sets a custom OpenTelemetry instance for distributed tracing.
     * Defaults to {@link GlobalOpenTelemetry} if not provided.
     *
     * @param openTelemetry the OpenTelemetry instance to use
     * @return this builder
     */
    public KafkaProducerBuilder withOpenTelemetry(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        return this;
    }

    /**
     * Builds a ready-to-use {@link KafkaProducer}.
     * Validates config, checks Schema Registry connectivity, wires all components.
     *
     * @throws ConfigurationException if required config is missing, invalid, or SR is unreachable
     */
    public KafkaProducer build() {
        if (clusterConfig == null) {
            throw new ConfigurationException("clusterConfig is required");
        }

        validateAuthConfigForClusterType(clusterConfig);

        SchemaRegistryClient srClient = createSchemaRegistryClient(clusterConfig);
        validateSchemaRegistryConnectivity(srClient, clusterConfig);

        MeterRegistry registry = meterRegistry != null ? meterRegistry : new SimpleMeterRegistry();
        OpenTelemetry otel = openTelemetry != null ? openTelemetry : GlobalOpenTelemetry.get();

        MetricsEmitter metricsEmitter = new MetricsEmitter(registry, clusterConfig.getName());
        TraceEmitter traceEmitter = new TraceEmitter(otel);
        ObservabilityContext observabilityContext = new ObservabilityContext(metricsEmitter, traceEmitter);

        SchemaValidator schemaValidator = new ConfluentSchemaValidator(
                srClient, clusterConfig.getSchemaRegistryConfig());

        KafkaClientFactory factory = new KafkaClientFactory();
        org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer =
                factory.create(clusterConfig);

        log.info("KafkaProducer built: cluster={} type={}",
                clusterConfig.getName(), clusterConfig.getType());

        return new KafkaProducerImpl(kafkaProducer, schemaValidator, observabilityContext);
    }

    private void validateAuthConfigForClusterType(ClusterConfig config) {
        ClusterType type = config.getType();
        AuthConfig auth = config.getAuthConfig();

        if (type == ClusterType.STRIMZI_MTLS) {
            if (auth.getTlsKeystorePath() == null) {
                throw new ConfigurationException(
                        "tlsKeystorePath is required for STRIMZI_MTLS cluster type");
            }
            if (auth.getTlsTruststorePath() == null) {
                throw new ConfigurationException(
                        "tlsTruststorePath is required for STRIMZI_MTLS cluster type");
            }
        }

        if (type == ClusterType.STRIMZI_SCRAM) {
            if (auth.getScramUsername() == null) {
                throw new ConfigurationException(
                        "scramUsername is required for STRIMZI_SCRAM cluster type");
            }
            if (auth.getScramPassword() == null) {
                throw new ConfigurationException(
                        "scramPassword is required for STRIMZI_SCRAM cluster type");
            }
        }
    }

    private SchemaRegistryClient createSchemaRegistryClient(ClusterConfig config) {
        String url = config.getSchemaRegistryConfig().getUrl();
        int cacheCapacity = config.getSchemaRegistryConfig().getCacheMaxSize();
        return new CachedSchemaRegistryClient(url, cacheCapacity);
    }

    private void validateSchemaRegistryConnectivity(
            SchemaRegistryClient srClient, ClusterConfig config) {
        String url = config.getSchemaRegistryConfig().getUrl();
        try {
            srClient.getAllSubjects();
            log.debug("Schema Registry connectivity verified: url={}", url);
        } catch (IOException | RestClientException e) {
            throw new ConfigurationException(
                    "Schema Registry unreachable at " + url + ": " + e.getMessage(), e);
        }
    }
}
