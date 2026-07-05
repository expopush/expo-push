package dev.expopush.api;

import java.util.Map;

/**
 * Optional per-notification delivery settings, mirroring the Expo push message fields
 * beyond title/body. All components are nullable — null means "let Expo use its default".
 *
 * <p>{@code data} is delivered to the app as the notification's custom JSON payload. It may
 * contain application-identifying or user-identifying information, so persistent backends
 * (SQS, H2) encrypt it at rest exactly like title and body — as is {@code subtitle}, which
 * is content. The remaining fields (channel, sound, ttl, badge, priority) are delivery
 * mechanics and are stored in plaintext.
 *
 * <p>Note: {@code data} values must be JSON-serializable (strings, numbers, booleans,
 * lists, maps). The result callback does NOT echo these options back; use
 * {@code NotificationCommand.metadata} for values you need returned with the outcome.
 *
 * @param data      Custom JSON payload delivered to the app ({@code notification.request.content.data}).
 * @param channelId Android notification channel ID.
 * @param sound     iOS sound name; {@code "default"} plays the standard sound.
 * @param ttl       Seconds the notification may be retried/stored if the device is offline.
 * @param badge     iOS app icon badge count; {@code 0} clears the badge.
 * @param subtitle  iOS secondary title shown below {@code title}.
 * @param priority  Delivery priority; see {@link NotificationPriority}.
 */
public record NotificationOptions(
    Map<String, Object> data,
    String channelId,
    String sound,
    Integer ttl,
    Integer badge,
    String subtitle,
    NotificationPriority priority
) {

    private static final NotificationOptions NONE =
        new NotificationOptions(null, null, null, null, null, null, null);

    /** All-defaults instance: every field null, Expo behavior unchanged. */
    public static NotificationOptions none() {
        return NONE;
    }

    @Override
    public String toString() {
        return "NotificationOptions[" +
            "data=" + (data == null ? null : (LogMasker.isMaskingEnabled() ? "[MASKED]" : data)) +
            ", channelId=" + channelId +
            ", sound=" + sound +
            ", ttl=" + ttl +
            ", badge=" + badge +
            ", subtitle=" + LogMasker.mask(subtitle) +
            ", priority=" + priority +
            ']';
    }
}
