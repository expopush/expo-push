package dev.expopush.autoconfigure;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;

/**
 * Default {@link AsyncNotificationService} that delegates to the configured
 * {@link NotificationBackend}.
 */
public class DefaultAsyncNotificationService implements AsyncNotificationService {

    private final NotificationBackend backend;
    private final NotificationHandlerRegistry registry;

    public DefaultAsyncNotificationService(NotificationBackend backend, NotificationHandlerRegistry registry) {
        this.backend = backend;
        this.registry = registry;
    }

    @Override
    public void enqueue(NotificationCommand command) {
        validate(command);
        backend.submit(command);
    }

    private void validate(NotificationCommand command) {
        if (command == null) {
            throw new NotificationSubmissionException("NotificationCommand must not be null");
        }
        requireNonBlank(command.pushToken(),     "pushToken");
        requireNonBlank(command.correlationId(), "correlationId");
        requireNonBlank(command.handlerId(),     "handlerId");
        // Fail fast on a handler ID that nothing can route: discovering the mismatch here,
        // at the call site, beats discovering it at consume time on another node where the
        // outcome would be stranded.
        if (registry.getHandler(command.handlerId()) == null) {
            throw new NotificationSubmissionException(
                "No NotificationResultHandler is registered for handlerId='" + command.handlerId()
                    + "'. Register a bean whose handlerId() returns this value before enqueuing.");
        }
        validateOptions(command.options());
    }

    private static void validateOptions(dev.expopush.api.NotificationOptions options) {
        if (options == null) return;
        if (options.ttl() != null && options.ttl() <= 0) {
            throw new NotificationSubmissionException(
                "NotificationOptions.ttl must be positive (got " + options.ttl() + ")");
        }
        if (options.badge() != null && options.badge() < 0) {
            throw new NotificationSubmissionException(
                "NotificationOptions.badge must be >= 0 (got " + options.badge() + ")");
        }
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new NotificationSubmissionException(
                "NotificationCommand." + field + " must not be null or blank");
        }
    }
}
