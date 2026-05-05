package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultNotificationHandlerRegistryTest {

    private static NotificationResultHandler handler(String id) {
        return new NotificationResultHandler() {
            @Override public String handlerId() { return id; }
            @Override public void handleResult(NotificationResult r) {
                // no-op test stub — intentionally empty
            }
        };
    }

    @Test
    void emptyHandlerListCreatesEmptyRegistry() {
        DefaultNotificationHandlerRegistry registry = new DefaultNotificationHandlerRegistry(List.of());
        assertThat(registry.getHandler("any")).isNull();
    }

    @Test
    void singleHandlerIsDiscoverable() {
        NotificationResultHandler h = handler("my-handler");
        DefaultNotificationHandlerRegistry registry = new DefaultNotificationHandlerRegistry(List.of(h));
        assertThat(registry.getHandler("my-handler")).isSameAs(h);
    }

    @Test
    void multipleHandlersAllDiscoverable() {
        NotificationResultHandler h1 = handler("handler-1");
        NotificationResultHandler h2 = handler("handler-2");
        NotificationResultHandler h3 = handler("handler-3");

        DefaultNotificationHandlerRegistry registry = new DefaultNotificationHandlerRegistry(
            List.of(h1, h2, h3));

        assertThat(registry.getHandler("handler-1")).isSameAs(h1);
        assertThat(registry.getHandler("handler-2")).isSameAs(h2);
        assertThat(registry.getHandler("handler-3")).isSameAs(h3);
    }

    @Test
    void unknownHandlerIdReturnsNull() {
        DefaultNotificationHandlerRegistry registry = new DefaultNotificationHandlerRegistry(
            List.of(handler("known")));
        assertThat(registry.getHandler("unknown")).isNull();
    }

    @Test
    void duplicateHandlerIdThrowsIllegalState() {
        NotificationResultHandler h1 = handler("duplicate-id");
        NotificationResultHandler h2 = handler("duplicate-id");
        List<NotificationResultHandler> handlers = List.of(h1, h2);

        assertThatThrownBy(() -> new DefaultNotificationHandlerRegistry(handlers))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("duplicate-id")
            .hasMessageContaining("unique handlerId");
    }

    @Test
    void duplicateExceptionMessageContainsBothClassNames() {
        NotificationResultHandler h1 = handler("dup");
        NotificationResultHandler h2 = handler("dup");
        List<NotificationResultHandler> handlers = List.of(h1, h2);

        assertThatThrownBy(() -> new DefaultNotificationHandlerRegistry(handlers))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining(h1.getClass().getName());
    }
}
