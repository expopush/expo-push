package dev.expopush.core.exception;

/**
 * Thrown when Expo returns HTTP 429. Retryable; when Expo supplied a {@code Retry-After}
 * header, {@link #getRetryAfterSeconds()} carries it so the retry can honor Expo's own
 * pacing instead of blind exponential backoff.
 */
public class ExpoRateLimitException extends RuntimeException {

    /** Seconds Expo asked us to wait, or null when no (parseable) Retry-After was sent. */
    private final transient Long retryAfterSeconds;

    public ExpoRateLimitException(String message) {
        this(message, null);
    }

    public ExpoRateLimitException(String message, Long retryAfterSeconds) {
        super(message);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public Long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }
}
