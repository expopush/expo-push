package dev.expopush.api;

/**
 * The five terminal outcomes reported to a {@link NotificationResultHandler}.
 */
public enum NotificationOutcome {

    /** Expo confirmed delivery via push receipt. */
    ACCEPTED,

    /**
     * The device token is no longer valid ({@code DeviceNotRegistered}).
     * The handler should deactivate the token.
     */
    REJECTED,

    /**
     * An application-level problem prevented delivery (e.g. message too large,
     * malformed payload). The token is still valid; fix the content and retry.
     */
    INVALID,

    /**
     * The message reached Expo (a ticket was issued) but a delivery receipt was never
     * confirmed within the retry window. The notification may or may not have been
     * delivered — retrying risks a duplicate push to the user.
     *
     * <p>This outcome also occurs in the SQS backend when the JVM is killed in the narrow
     * window between Expo accepting a batch and the SQS messages being deleted. Because
     * Expo's {@code expoPushIdempotencyKey} operates at the batch level and batch
     * composition is not guaranteed to be stable across SQS redeliveries, exactly-once
     * delivery cannot be guaranteed. Handlers should treat {@code correlationId} as the
     * deduplication key and be idempotent on it.
     */
    UNKNOWN,

    /**
     * The message never reached Expo — all send attempts were exhausted. No duplicate
     * risk; safe to retry unconditionally.
     */
    FAILED
}
