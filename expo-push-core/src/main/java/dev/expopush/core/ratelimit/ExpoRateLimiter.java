package dev.expopush.core.ratelimit;

/**
 * Abstraction over a rate limiter that controls throughput of Expo API calls.
 *
 * <p>The default implementation, {@link LocalExpoRateLimiter}, uses a Resilience4j
 * {@code RateLimiter} scoped to the current JVM node. For multi-node deployments where
 * the aggregate rate limit must be shared, replace this bean with a Redis-backed
 * implementation — consumer code requires no changes.
 */
public interface ExpoRateLimiter {

    /**
     * Acquires permission to make one Expo API call. Blocks until a permit is
     * available or the configured timeout elapses.
     *
     * @throws io.github.resilience4j.ratelimiter.RequestNotPermitted if the timeout
     *         elapses before a permit is granted
     */
    void acquire();
}
