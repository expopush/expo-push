package dev.expopush.backend.sqs.message;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationResult;

import java.util.Map;

/**
 * Payload of an SQS message on the Push Notification Queue (PNQ).
 *
 * <p>{@code correlationId} and {@code metadata} are carried from the originating
 * {@link NotificationCommand} unchanged and echoed back in the final
 * {@link NotificationResult}.
 */
public record PushNotificationSqsMessage(
    String pushToken,
    String title,
    String body,
    String correlationId,
    Map<String, String> metadata,
    String handlerId,
    int schemaVersion
) implements SqsNotificationMessage {

    /** Convenience constructor producing the current schema version. */
    public PushNotificationSqsMessage(
        String pushToken, String title, String body,
        String correlationId, Map<String, String> metadata, String handlerId
    ) {
        this(pushToken, title, body, correlationId, metadata, handlerId, CURRENT_SCHEMA_VERSION);
    }
}
