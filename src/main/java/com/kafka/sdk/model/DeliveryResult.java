package com.kafka.sdk.model;

/** Immutable outcome of a single produce operation returned to the calling application. */
public final class DeliveryResult {

    private final boolean success;
    private final String topic;
    private final String correlationId;
    private final int partition;
    private final long offset;
    private final ErrorCode errorCode;
    private final String errorMessage;

    private DeliveryResult(
            boolean success,
            String topic,
            String correlationId,
            int partition,
            long offset,
            ErrorCode errorCode,
            String errorMessage) {
        this.success = success;
        this.topic = topic;
        this.correlationId = correlationId;
        this.partition = partition;
        this.offset = offset;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Creates a successful delivery result with partition and offset information.
     *
     * @param topic         the topic the message was written to
     * @param correlationId the SDK-generated correlation identifier for this produce call
     * @param partition     the Kafka partition the message landed on
     * @param offset        the Kafka offset of the delivered message
     */
    public static DeliveryResult success(
            String topic, String correlationId, int partition, long offset) {
        return new DeliveryResult(true, topic, correlationId, partition, offset, null, null);
    }

    /**
     * Creates a failed delivery result with an error code and descriptive message.
     *
     * @param topic         the topic the produce was attempted against
     * @param correlationId the SDK-generated correlation identifier for this produce call
     * @param errorCode     the structured error category
     * @param errorMessage  the human-readable error description
     */
    public static DeliveryResult failure(
            String topic, String correlationId, ErrorCode errorCode, String errorMessage) {
        return new DeliveryResult(false, topic, correlationId, -1, -1, errorCode, errorMessage);
    }

    /** Returns {@code true} if the message was successfully delivered to Kafka. */
    public boolean isSuccess() { return success; }

    /** Returns the Kafka topic the message was produced (or attempted) to. */
    public String getTopic() { return topic; }

    /**
     * Returns the SDK-generated correlation ID for this produce call.
     * Correlates the {@link DeliveryResult} with metrics, traces, and log entries.
     */
    public String getCorrelationId() { return correlationId; }

    /** Valid only when {@link #isSuccess()} is true. */
    public int getPartition() { return partition; }

    /** Valid only when {@link #isSuccess()} is true. */
    public long getOffset() { return offset; }

    /** Valid only when {@link #isSuccess()} is false. */
    public ErrorCode getErrorCode() { return errorCode; }

    /** Valid only when {@link #isSuccess()} is false. */
    public String getErrorMessage() { return errorMessage; }

    @Override
    public String toString() {
        if (success) {
            return "DeliveryResult{success, topic=" + topic
                    + ", partition=" + partition + ", offset=" + offset
                    + ", correlationId=" + correlationId + "}";
        }
        return "DeliveryResult{failure, topic=" + topic
                + ", errorCode=" + errorCode
                + ", correlationId=" + correlationId + "}";
    }
}
