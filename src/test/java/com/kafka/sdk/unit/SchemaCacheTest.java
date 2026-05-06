package com.kafka.sdk.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.sdk.schema.SchemaCache;
import org.junit.jupiter.api.Test;

class SchemaCacheTest {

    @Test
    void get_returnsCachedSchemaOnHit() {
        SchemaCache cache = new SchemaCache(10, 60);
        Object schema = new Object();
        cache.put("orders-value", schema);

        Object result = cache.get("orders-value");

        assertThat(result).isSameAs(schema);
        assertThat(cache.getHitCount()).isEqualTo(1);
        assertThat(cache.getMissCount()).isEqualTo(0);
    }

    @Test
    void get_returnsNullOnMiss() {
        SchemaCache cache = new SchemaCache(10, 60);

        Object result = cache.get("unknown-subject");

        assertThat(result).isNull();
        assertThat(cache.getMissCount()).isEqualTo(1);
    }

    @Test
    void get_returnsNullAfterTtlExpiry() throws InterruptedException {
        SchemaCache cache = new SchemaCache(10, 1); // 1-second TTL
        cache.put("orders-value", new Object());

        Thread.sleep(1100); // wait for TTL to expire

        assertThat(cache.get("orders-value")).isNull();
    }

    @Test
    void put_evictsEldestEntryWhenMaxSizeExceeded() {
        SchemaCache cache = new SchemaCache(2, 60); // max 2 entries
        Object schema1 = new Object();
        Object schema2 = new Object();
        Object schema3 = new Object();

        cache.put("subject-1", schema1);
        cache.put("subject-2", schema2);
        cache.put("subject-3", schema3); // should evict subject-1

        assertThat(cache.get("subject-3")).isSameAs(schema3);
        assertThat(cache.get("subject-2")).isSameAs(schema2);
        // subject-1 was evicted (LRU)
        assertThat(cache.get("subject-1")).isNull();
    }
}
