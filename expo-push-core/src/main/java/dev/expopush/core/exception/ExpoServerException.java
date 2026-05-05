package dev.expopush.core.exception;

/** Thrown when Expo returns HTTP 5xx. Retryable with exponential backoff. */
public class ExpoServerException extends RuntimeException {
    public ExpoServerException(String message) { super(message); }
}
