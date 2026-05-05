package dev.expopush.backend.api;

import dev.expopush.api.NotificationResultHandler;
import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationSubmissionException;

/**
 * SPI for the async notification backend.
 *
 * <p>A backend owns the complete delivery lifecycle for a submitted command:
 * transport to Expo, receipt follow-up, and routing the terminal outcome to the
 * registered {@link NotificationResultHandler}.
 *
 * <p>The current implementation is {@code expo-push-backend-sqs}. Additional
 * implementations (JDBC, in-memory) can be added without changing calling code.
 *
 * <p>Implementations that manage threads or connections should also implement
 * {@link org.springframework.context.SmartLifecycle} to integrate with Spring's
 * application lifecycle.
 */
public interface NotificationBackend {

    /**
     * Accepts a command for asynchronous delivery.
     *
     * @throws NotificationSubmissionException if the command cannot be accepted
     */
    void submit(NotificationCommand command);
}
