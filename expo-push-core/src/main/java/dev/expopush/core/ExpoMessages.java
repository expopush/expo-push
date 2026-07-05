package dev.expopush.core;

import dev.expopush.api.NotificationOptions;
import dev.expopush.api.NotificationPriority;
import dev.expopush.core.api.model.PushMessage;
import dev.expopush.core.api.model.PushSound;

import java.util.List;
import java.util.Map;

/**
 * Maps the starter's neutral notification fields onto the generated Expo {@link PushMessage}.
 * The single place where {@link NotificationOptions} meets the Expo API model — all three
 * backends build their requests through here.
 */
public final class ExpoMessages {

    private ExpoMessages() {}

    /**
     * Builds a single-recipient Expo push message. {@code options} may be null; null option
     * fields are left unset so Expo applies its defaults.
     *
     * @param data decrypted custom payload, passed separately from {@code options} because
     *             persistent backends carry it encrypted and re-materialize it at send time
     */
    public static PushMessage toPushMessage(
        String pushToken,
        String title,
        String body,
        Map<String, Object> data,
        NotificationOptions options
    ) {
        PushMessage pm = new PushMessage();
        pm.setTo(List.of(pushToken));
        pm.setTitle(title);
        pm.setBody(body);
        pm.setPriority(mapPriority(options == null ? null : options.priority()));
        if (data != null) {
            pm.setData(data);
        }
        if (options != null) {
            if (options.channelId() != null) pm.setChannelId(options.channelId());
            if (options.subtitle() != null) pm.setSubtitle(options.subtitle());
            if (options.ttl() != null) pm.setTtl(options.ttl());
            if (options.badge() != null) pm.setBadge(options.badge());
            if (options.sound() != null) {
                PushSound sound = new PushSound();
                sound.setName(options.sound());
                pm.setSound(sound);
            }
        }
        return pm;
    }

    /** Convenience overload where {@code data} rides inside {@code options} (local backend). */
    public static PushMessage toPushMessage(
        String pushToken, String title, String body, NotificationOptions options
    ) {
        return toPushMessage(pushToken, title, body,
            options == null ? null : options.data(), options);
    }

    private static PushMessage.PriorityEnum mapPriority(NotificationPriority priority) {
        if (priority == null) return PushMessage.PriorityEnum.DEFAULT;
        return switch (priority) {
            case DEFAULT -> PushMessage.PriorityEnum.DEFAULT;
            case NORMAL -> PushMessage.PriorityEnum.NORMAL;
            case HIGH -> PushMessage.PriorityEnum.HIGH;
        };
    }
}
