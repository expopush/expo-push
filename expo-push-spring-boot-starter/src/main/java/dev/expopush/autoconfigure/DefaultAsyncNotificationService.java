package dev.expopush.autoconfigure;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;

/**
 * Default {@link AsyncNotificationService} that delegates to the configured
 * {@link NotificationBackend}.
 */
public class DefaultAsyncNotificationService implements AsyncNotificationService {

    private final NotificationBackend backend;

    public DefaultAsyncNotificationService(NotificationBackend backend) {
        this.backend = backend;
    }

    @Override
    public void enqueue(NotificationCommand command) {
        validate(command);
        backend.submit(command);
    }

    private static void validate(NotificationCommand command) {
        if (command == null) {
            throw new NotificationSubmissionException("NotificationCommand must not be null");
        }
        requireNonBlank(command.pushToken(),     "pushToken");
        requireNonBlank(command.correlationId(), "correlationId");
        requireNonBlank(command.handlerId(),     "handlerId");
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new NotificationSubmissionException(
                "NotificationCommand." + field + " must not be null or blank");
        }
    }
}
