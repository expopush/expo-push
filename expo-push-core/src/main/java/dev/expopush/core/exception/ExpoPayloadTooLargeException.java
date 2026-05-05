package dev.expopush.core.exception;

/** Thrown when Expo returns HTTP 413. Non-retryable — batch exceeds 100 messages or 4 KB total. */
public class ExpoPayloadTooLargeException extends RuntimeException {
    public ExpoPayloadTooLargeException(String message) { super(message); }
}
