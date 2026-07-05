package dev.expopush.api;

/**
 * Delivery priority for a push notification, mirroring Expo's {@code priority} field.
 * Kept as a neutral type so callers never depend on generated Expo API models.
 */
public enum NotificationPriority {

    /** Let Expo choose ({@code normal} for Android, {@code high} for iOS). */
    DEFAULT,

    /** May be batched/delayed by the OS to conserve battery. */
    NORMAL,

    /** Wakes the device immediately; use for time-sensitive notifications. */
    HIGH
}
