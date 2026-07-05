package dev.expopush.core;

import dev.expopush.api.NotificationOptions;
import dev.expopush.api.NotificationPriority;
import dev.expopush.core.api.model.PushMessage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExpoMessagesTest {

    @Test
    void nullOptionsProducesMinimalMessageWithDefaultPriority() {
        PushMessage pm = ExpoMessages.toPushMessage("tok", "title", "body", null);

        assertThat(pm.getTo()).isEqualTo(List.of("tok"));
        assertThat(pm.getTitle()).isEqualTo("title");
        assertThat(pm.getBody()).isEqualTo("body");
        assertThat(pm.getPriority()).isEqualTo(PushMessage.PriorityEnum.DEFAULT);
        assertThat(pm.getChannelId()).isNull();
        assertThat(pm.getSubtitle()).isNull();
        assertThat(pm.getTtl()).isNull();
        assertThat(pm.getBadge()).isNull();
        assertThat(pm.getSound()).isNull();
    }

    @Test
    void allOptionsMapOntoPushMessage() {
        NotificationOptions options = new NotificationOptions(
            Map.of("screen", "orders", "orderId", 42),
            "order-updates",
            "default",
            3600,
            7,
            "Order shipped",
            NotificationPriority.HIGH);

        PushMessage pm = ExpoMessages.toPushMessage("tok", "title", "body", options);

        assertThat(pm.getData()).containsEntry("screen", "orders").containsEntry("orderId", 42);
        assertThat(pm.getChannelId()).isEqualTo("order-updates");
        assertThat(pm.getSound().getName()).isEqualTo("default");
        assertThat(pm.getTtl()).isEqualTo(3600);
        assertThat(pm.getBadge()).isEqualTo(7);
        assertThat(pm.getSubtitle()).isEqualTo("Order shipped");
        assertThat(pm.getPriority()).isEqualTo(PushMessage.PriorityEnum.HIGH);
    }

    @Test
    void explicitDataArgumentWinsOverOptionsData() {
        // The persistent-backend overload passes decrypted data separately.
        NotificationOptions options = new NotificationOptions(
            null, null, null, null, null, null, NotificationPriority.NORMAL);

        PushMessage pm = ExpoMessages.toPushMessage(
            "tok", "t", "b", Map.of("k", "v"), options);

        assertThat(pm.getData()).containsEntry("k", "v");
        assertThat(pm.getPriority()).isEqualTo(PushMessage.PriorityEnum.NORMAL);
    }

    @Test
    void badgeZeroIsPreservedToClearTheBadge() {
        NotificationOptions options = new NotificationOptions(
            null, null, null, null, 0, null, null);

        PushMessage pm = ExpoMessages.toPushMessage("tok", "t", "b", options);

        assertThat(pm.getBadge()).isZero();
    }
}
