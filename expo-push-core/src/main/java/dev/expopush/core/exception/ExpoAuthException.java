package dev.expopush.core.exception;

/** Thrown when Expo returns HTTP 401. Non-retryable — check the configured access token. */
public class ExpoAuthException extends RuntimeException {
    public ExpoAuthException(String message) { super(message); }
}
