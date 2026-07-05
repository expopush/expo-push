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
    int schemaVersion,
    /* Delivery options (schema version 2+). dataJson and subtitle are content and are
       encrypted at rest like title/body; the rest are delivery mechanics, plaintext. */
    String dataJson,
    String subtitle,
    String channelId,
    String sound,
    Integer ttl,
    Integer badge,
    String priority
) implements SqsNotificationMessage {

    /** Convenience constructor for messages without delivery options (current version). */
    public PushNotificationSqsMessage(
        String pushToken, String title, String body,
        String correlationId, Map<String, String> metadata, String handlerId
    ) {
        this(pushToken, title, body, correlationId, metadata, handlerId, CURRENT_SCHEMA_VERSION,
            null, null, null, null, null, null, null);
    }
}
