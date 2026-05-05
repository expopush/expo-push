package dev.expopush.api;

/**
 * Thrown when a notification command cannot be submitted to the async delivery pipeline.
 * Expo has not been contacted; no downstream callback will fire for this command.
 */
public class NotificationSubmissionException extends RuntimeException {

    public NotificationSubmissionException(String message) {
        super(message);
    }

    public NotificationSubmissionException(String message, Throwable cause) {
        super(message, cause);
    }
}
