package dev.expopush.backend.sqs.message;

import java.util.Map;

/**
 * Common fields shared by every SQS notification message regardless of which queue it
 * lives on. Implemented by {@link PushNotificationSqsMessage} and
 * {@link PushReceiptSqsMessage}.
 */
public interface SqsNotificationMessage {
    String handlerId();
    String correlationId();
    Map<String, String> metadata();
    String pushToken();
    String title();
    String body();
}
