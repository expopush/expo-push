package dev.expopush.core.ratelimit;

import io.github.resilience4j.ratelimiter.RateLimiter;

/**
 * Node-local {@link ExpoRateLimiter} backed by a Resilience4j {@link RateLimiter}.
 * Suitable for single-node deployments.
 */
public class LocalExpoRateLimiter implements ExpoRateLimiter {

    private final RateLimiter rateLimiter;

    public LocalExpoRateLimiter(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void acquire() {
        rateLimiter.acquirePermission();
    }
}
