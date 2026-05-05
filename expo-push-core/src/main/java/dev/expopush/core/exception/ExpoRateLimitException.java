package dev.expopush.core.exception;

/** Thrown when Expo returns HTTP 429. Retryable with exponential backoff. */
public class ExpoRateLimitException extends RuntimeException {
    public ExpoRateLimitException(String message) { super(message); }
}
