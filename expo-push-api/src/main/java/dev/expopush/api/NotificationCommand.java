package dev.expopush.api;

import java.util.Map;

/**
 * A request to send a single push notification asynchronously via the Expo push pipeline.
 *
 * <p>{@code correlationId} is an opaque string carried through the entire pipeline and
 * echoed back in the {@link NotificationResult}. Callers may use it to correlate
 * results with their own records (e.g. a database row ID or a composite key).
 *
 * <p>{@code metadata} is an optional bag of key-value strings that travels alongside the
 * notification through every queue hop and is also echoed in the result. Use it for
 * application-specific context that must survive the async round-trip without being
 * coupled to the starter's public API (e.g. participant IDs, device registration IDs).
 * Values must be non-null strings; the map itself may be null or empty.
 *
 * @param pushToken     Expo push token for the target device.
 * @param title         Notification title.
 * @param body          Notification body.
 * @param correlationId Opaque caller-assigned correlation key; echoed in the result.
 * @param metadata      Optional application context; echoed in the result.
 * @param handlerId     Stable identifier of the {@link NotificationResultHandler} to
 *                      invoke when the notification lifecycle completes.
 */
public record NotificationCommand(
    String pushToken,
    String title,
    String body,
    String correlationId,
    Map<String, String> metadata,
    String handlerId
) {
    @Override
    public String toString() {
        return "NotificationCommand[" +
            "pushToken=" + pushToken +
            ", title=" + LogMasker.mask(title) +
            ", body=" + LogMasker.mask(body) +
            ", correlationId=" + correlationId +
            ", metadata=" + (LogMasker.isMaskingEnabled() ? "[MASKED]" : metadata) +
            ", handlerId=" + handlerId +
            ']';
    }
}
