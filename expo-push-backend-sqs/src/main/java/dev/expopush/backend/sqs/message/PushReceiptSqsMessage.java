package dev.expopush.backend.sqs.message;

import java.util.Map;

/**
 * Payload of an SQS message on the Receipt Queue (RQ).
 *
 * <p>Written by the PNQ consumer after Expo issues a ticket. Sent with a configurable
 * delay (default 15 minutes) so that Expo has time to process the notification before
 * the receipt consumer checks.
 */
public record PushReceiptSqsMessage(
    String ticketId,
    String pushToken,
    String correlationId,
    Map<String, String> metadata,
    String handlerId,
    String title,
    String body,
    int schemaVersion
) implements SqsNotificationMessage {

    /** Convenience constructor producing the current schema version. */
    public PushReceiptSqsMessage(
        String ticketId, String pushToken, String correlationId,
        Map<String, String> metadata, String handlerId, String title, String body
    ) {
        this(ticketId, pushToken, correlationId, metadata, handlerId, title, body, CURRENT_SCHEMA_VERSION);
    }
}
