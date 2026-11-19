package com.example.jobscheduler.lease;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Redis-backed leases. The key {@code lease:<jobId>} holds the owning worker id and expires
 * unless renewed. Renew and release are compare-and-set so a slow worker cannot extend or
 * delete a lease that now belongs to someone else.
 */
@Component
public class LeaseStore {

    private static final RedisScript<Long> RENEW = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
              return redis.call('pexpire', KEYS[1], ARGV[2])
            end
            return 0
            """, Long.class);

    private static final RedisScript<Long> RELEASE = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
              return redis.call('del', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;
    private final long ttlMs;

    public LeaseStore(StringRedisTemplate redis, @Value("${scheduler.lease-ttl-ms:10000}") long ttlMs) {
        this.redis = redis;
        this.ttlMs = ttlMs;
    }

    public void acquire(UUID jobId, String owner) {
        redis.opsForValue().set(key(jobId), owner, Duration.ofMillis(ttlMs));
    }

    /** @return false if the lease is no longer held by {@code owner}. */
    public boolean renew(UUID jobId, String owner) {
        Long result = redis.execute(RENEW, List.of(key(jobId)), owner, String.valueOf(ttlMs));
        return result != null && result == 1L;
    }

    public void release(UUID jobId, String owner) {
        redis.execute(RELEASE, List.of(key(jobId)), owner);
    }

    public boolean isHeld(UUID jobId) {
        return Boolean.TRUE.equals(redis.hasKey(key(jobId)));
    }

    private static String key(UUID jobId) {
        return "lease:" + jobId;
    }
}
