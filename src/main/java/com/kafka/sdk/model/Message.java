package com.kafka.sdk.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Represents a single produce request: topic, optional key, payload, and optional headers. */
public final class Message {

    private final String topic;
    private final byte[] key;
    private final Object payload;
    private final Map<String, String> headers;

    private Message(Builder builder) {
        this.topic = builder.topic;
        this.key = builder.key;
        this.payload = builder.payload;
        this.headers = Collections.unmodifiableMap(new HashMap<>(builder.headers));
    }

    /**
     * Creates a new builder for a message targeting the given topic.
     *
     * @param topic the destination Kafka topic; must not be null or blank
     * @return a new builder
     */
    public static Builder forTopic(String topic) {
        return new Builder(topic);
    }

    /** Returns the destination Kafka topic. */
    public String getTopic() { return topic; }

    /** Returns the optional message key, or {@code null} if not set. */
    public byte[] getKey() { return key; }

    /** Returns the message payload. Supported types: {@link String}, {@code byte[]}, Avro {@code GenericRecord}. */
    public Object getPayload() { return payload; }

    /** Returns an unmodifiable header map; never null, may be empty. */
    public Map<String, String> getHeaders() { return headers; }

    public static final class Builder {
        private final String topic;
        private byte[] key;
        private Object payload;
        private final Map<String, String> headers = new HashMap<>();

        private Builder(String topic) {
            Objects.requireNonNull(topic, "topic must not be null");
            if (topic.isBlank()) {
                throw new IllegalArgumentException("topic must not be blank");
            }
            this.topic = topic;
        }

        /** Sets the optional partition key for this message. */
        public Builder key(byte[] key) {
            this.key = key;
            return this;
        }

        /**
         * Sets the message payload. Supported types: {@link String}, {@code byte[]},
         * or Avro {@code GenericRecord} (validated against the registered schema).
         */
        public Builder payload(Object payload) {
            this.payload = payload;
            return this;
        }

        /** Adds a single header. Null keys or values are silently ignored. */
        public Builder header(String key, String value) {
            if (key != null && value != null) {
                headers.put(key, value);
            }
            return this;
        }

        /** Adds all entries from the given map. Null keys or values are silently ignored. */
        public Builder headers(Map<String, String> headers) {
            if (headers != null) {
                headers.forEach((k, v) -> {
                    if (k != null && v != null) {
                        this.headers.put(k, v);
                    }
                });
            }
            return this;
        }

        /**
         * Builds the {@link Message}.
         *
         * @throws NullPointerException if payload was not set
         */
        public Message build() {
            Objects.requireNonNull(payload, "payload must not be null");
            return new Message(this);
        }
    }
}
