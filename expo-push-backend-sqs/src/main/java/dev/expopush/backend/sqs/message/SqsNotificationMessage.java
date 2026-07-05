package dev.expopush.backend.sqs.message;

import java.util.Map;

/**
 * Common fields shared by every SQS notification message regardless of which queue it
 * lives on. Implemented by {@link PushNotificationSqsMessage} and
 * {@link PushReceiptSqsMessage}.
 */
public interface SqsNotificationMessage {

    /**
     * The queue-message schema version currently produced. Messages written before
     * versioning was introduced deserialize with {@code schemaVersion() == 0} and are
     * accepted as equivalent to version 1. Messages with a HIGHER version than this
     * constant (produced by a newer deployment) must not be interpreted — consumers
     * leave them in the queue for redelivery/DLQ rather than misread or delete them.
     *
     * <p>History: 1 = token/title/body/correlation/metadata/handler;
     * 2 = adds delivery options (encrypted dataJson + subtitle, plaintext
     * channelId/sound/ttl/badge/priority) to the push message. Version-1 messages
     * deserialize with all option fields null and remain fully processable.
     */
    int CURRENT_SCHEMA_VERSION = 2;

    int schemaVersion();
    String handlerId();
    String correlationId();
    Map<String, String> metadata();
    String pushToken();
    String title();
    String body();
}
