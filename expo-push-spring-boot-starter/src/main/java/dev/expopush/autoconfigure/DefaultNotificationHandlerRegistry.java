package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationResultHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default {@link NotificationHandlerRegistry} that discovers all
 * {@link NotificationResultHandler} beans in the application context at startup.
 */
@Slf4j
public class DefaultNotificationHandlerRegistry implements NotificationHandlerRegistry {

    private final Map<String, NotificationResultHandler> handlers;

    public DefaultNotificationHandlerRegistry(List<NotificationResultHandler> handlers) {
        Map<String, NotificationResultHandler> map = new HashMap<>();
        for (NotificationResultHandler handler : handlers) {
            String id = handler.handlerId();
            NotificationResultHandler existing = map.put(id, handler);
            if (existing != null) {
                throw new IllegalStateException(
                    "Duplicate NotificationResultHandler handlerId '" + id + "' — conflicting beans: "
                        + existing.getClass().getName() + " and " + handler.getClass().getName()
                        + ". Each handler must return a unique handlerId().");
            }
        }
        this.handlers = Map.copyOf(map);
        log.info("Registered {} notification result handler(s): {}",
            this.handlers.size(), this.handlers.keySet());
    }

    @Override
    public NotificationResultHandler getHandler(String handlerId) {
        NotificationResultHandler handler = handlers.get(handlerId);
        if (handler == null) {
            log.error("No NotificationResultHandler found for handlerId='{}' — check for deployment mismatch",
                handlerId);
        }
        return handler;
    }
}
