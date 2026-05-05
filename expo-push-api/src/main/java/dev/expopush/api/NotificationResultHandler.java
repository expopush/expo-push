package dev.expopush.api;

/**
 * Strategy for handling the terminal outcome of a push notification lifecycle event.
 *
 * <p>Implementations are Spring beans discovered by the starter's auto-configuration
 * and looked up by their {@link #handlerId()} when a consumer processes a result.
 * The ID is serialised into every queue message so that any node in the cluster can
 * route the result to the correct handler without sharing in-memory state.
 *
 * <p>This callback is a best-effort terminal sink. If an implementation throws,
 * the infrastructure logs the failure but does not retry the callback.
 *
 * <p><strong>Idempotency requirement:</strong> implementations must be idempotent on
 * {@link NotificationResult#correlationId()}. The SQS backend provides at-least-once
 * delivery — a crash between Expo accepting a batch and the SQS delete completing will
 * cause redelivery and a possible duplicate push. Expo's batch-level idempotency key
 * cannot reliably prevent this because SQS does not guarantee stable batch composition
 * across redeliveries. Use {@code correlationId} as your deduplication key.
 */
public interface NotificationResultHandler {

    /**
     * A stable, unique identifier for this handler. Used as the routing key in queue
     * messages — must not change between deployments.
     */
    String handlerId();

    /**
     * Called when a push notification lifecycle event completes.
     */
    void handleResult(NotificationResult result);
}
