package dev.expopush.api;

import java.util.Map;

/**
 * Carries the result of one push notification lifecycle event to a
 * {@link NotificationResultHandler}.
 *
 * <p>Field semantics by outcome:
 * <ul>
 *   <li>{@code ACCEPTED}: {@code ticketId} set; {@code errorDetail} null.
 *   <li>{@code REJECTED}: {@code errorDetail} holds the Expo error code (e.g.
 *       {@code DeviceNotRegistered}); {@code ticketId} set when rejection was
 *       discovered at receipt stage, null at ticket stage.
 *   <li>{@code INVALID}: {@code errorDetail} holds the reason; {@code ticketId} set
 *       if discovered at receipt stage, null if at ticket stage.
 *   <li>{@code UNKNOWN}: {@code ticketId} set; {@code errorDetail} null.
 *   <li>{@code FAILED}: {@code errorDetail} holds the exception message; both
 *       {@code ticketId} and confirmation are absent.
 * </ul>
 *
 * <p>{@code correlationId} and {@code metadata} are echoed from the originating
 * {@link NotificationCommand} unchanged.
 */
public record NotificationResult(
    NotificationOutcome outcome,
    String handlerId,
    String correlationId,
    String pushToken,
    String title,
    String body,
    String ticketId,
    String errorDetail,
    Map<String, String> metadata
) {
    @Override
    public String toString() {
        return "NotificationResult[" +
            "outcome=" + outcome +
            ", handlerId=" + handlerId +
            ", correlationId=" + correlationId +
            ", pushToken=" + pushToken +
            ", title=" + LogMasker.mask(title) +
            ", body=" + LogMasker.mask(body) +
            ", ticketId=" + ticketId +
            ", errorDetail=" + errorDetail +
            ", metadata=" + (LogMasker.isMaskingEnabled() ? "[MASKED]" : metadata) +
            ']';
    }
}
