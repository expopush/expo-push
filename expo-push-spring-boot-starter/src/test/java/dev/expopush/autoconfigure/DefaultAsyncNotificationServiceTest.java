package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class DefaultAsyncNotificationServiceTest {

    @Mock
    private NotificationBackend backend;

    @InjectMocks
    private DefaultAsyncNotificationService service;

    // ─── Validation: null command ─────────────────────────────────────────────

    @Test
    void nullCommandThrowsSubmissionException() {
        assertThatThrownBy(() -> service.enqueue(null))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("null");
        verifyNoInteractions(backend);
    }

    // ─── Validation: pushToken ────────────────────────────────────────────────

    @Test
    void nullPushTokenThrowsSubmissionException() {
        NotificationCommand cmd = new NotificationCommand(null, "title", "body", "corr", Map.of(), "handler");
        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("pushToken");
    }

    @Test
    void blankPushTokenThrowsSubmissionException() {
        NotificationCommand cmd = new NotificationCommand("  ", "title", "body", "corr", Map.of(), "handler");
        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("pushToken");
    }

    // ─── Validation: correlationId ────────────────────────────────────────────

    @Test
    void nullCorrelationIdThrowsSubmissionException() {
        NotificationCommand cmd = new NotificationCommand("token", "title", "body", null, Map.of(), "handler");
        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("correlationId");
    }

    @Test
    void blankCorrelationIdThrowsSubmissionException() {
        NotificationCommand cmd = new NotificationCommand("token", "title", "body", "", Map.of(), "handler");
        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("correlationId");
    }

    // ─── Validation: handlerId ────────────────────────────────────────────────

    @Test
    void nullHandlerIdThrowsSubmissionException() {
        NotificationCommand cmd = new NotificationCommand("token", "title", "body", "corr", Map.of(), null);
        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("handlerId");
    }

    @Test
    void blankHandlerIdThrowsSubmissionException() {
        NotificationCommand cmd = new NotificationCommand("token", "title", "body", "corr", Map.of(), "");
        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("handlerId");
    }

    // ─── Happy path ───────────────────────────────────────────────────────────

    @Test
    void validCommandDelegatesToBackend() {
        NotificationCommand cmd = new NotificationCommand(
            "ExponentPushToken[abc]", "Hello", "World", "corr-123", Map.of("k", "v"), "handler-1");

        service.enqueue(cmd);

        verify(backend).submit(cmd);
    }

    @Test
    void validCommandWithNullOptionalFieldsStillDelegates() {
        NotificationCommand cmd = new NotificationCommand(
            "ExponentPushToken[abc]", null, null, "corr-456", null, "handler-1");

        service.enqueue(cmd);

        verify(backend).submit(cmd);
    }

    // ─── Backend exception propagation ───────────────────────────────────────

    @Test
    void backendExceptionPropagatesUnchanged() {
        NotificationCommand cmd = new NotificationCommand(
            "token", "title", "body", "corr", Map.of(), "handler");
        org.mockito.Mockito.doThrow(new RuntimeException("backend down")).when(backend).submit(cmd);

        assertThatThrownBy(() -> service.enqueue(cmd))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("backend down");
    }
}
