package com.kafka.sdk.schema;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-memory LRU cache for schema definitions.
 * Keyed by {@code subject} (latest version lookup).
 * Entries are evicted when exceeding {@code maxSize} (LRU) or after {@code ttlSeconds}.
 */
public class SchemaCache {

    private final int maxSize;
    private final long ttlSeconds;
    private final Map<String, CacheEntry> cache;
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    public SchemaCache(int maxSize, long ttlSeconds) {
        this.maxSize = maxSize;
        this.ttlSeconds = ttlSeconds;
        this.cache = new LinkedHashMap<>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
                return size() > maxSize;
            }
        };
    }

    /**
     * Retrieves a cached schema by subject.
     *
     * @return the cached schema object, or {@code null} if absent or expired
     */
    public synchronized Object get(String subject) {
        CacheEntry entry = cache.get(subject);
        if (entry == null || isExpired(entry)) {
            if (entry != null) {
                cache.remove(subject);
            }
            missCount.incrementAndGet();
            return null;
        }
        hitCount.incrementAndGet();
        return entry.schema;
    }

    /** Stores a schema in the cache under the given subject key. */
    public synchronized void put(String subject, Object schema) {
        cache.put(subject, new CacheEntry(schema, Instant.now()));
    }

    public long getHitCount() { return hitCount.get(); }

    public long getMissCount() { return missCount.get(); }

    private boolean isExpired(CacheEntry entry) {
        return Instant.now().isAfter(entry.cachedAt.plusSeconds(ttlSeconds));
    }

    private static final class CacheEntry {
        final Object schema;
        final Instant cachedAt;

        CacheEntry(Object schema, Instant cachedAt) {
            this.schema = schema;
            this.cachedAt = cachedAt;
        }
    }
}
