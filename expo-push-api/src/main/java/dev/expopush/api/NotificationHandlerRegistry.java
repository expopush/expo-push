package dev.expopush.api;

/**
 * Looks up registered {@link NotificationResultHandler} instances by handler ID.
 *
 * <p>The default implementation is registered by the starter's auto-configuration.
 * It discovers all {@link NotificationResultHandler} beans in the application context
 * at startup and makes them available for routing by consumer threads.
 */
public interface NotificationHandlerRegistry {

    /**
     * Returns the handler registered for {@code handlerId}, or {@code null} if none
     * is registered. A {@code null} return indicates a deployment mismatch — the
     * message was produced by a handler that no longer exists in the current codebase.
     */
    NotificationResultHandler getHandler(String handlerId);
}
