package dev.expopush.backend.api;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResultDispatcherTest {

    @Mock private NotificationHandlerRegistry registry;

    private static final NotificationResult RESULT = new NotificationResult(
        NotificationOutcome.ACCEPTED, "h-1", "corr-1", "tok", "t", "b", "ticket-1", null, Map.of());

    @Test
    void dispatchDeliversToHandler() {
        NotificationResultHandler handler = mock(NotificationResultHandler.class);
        when(registry.getHandler("h-1")).thenReturn(handler);

        new ResultDispatcher(registry).dispatch(RESULT);

        verify(handler).handleResult(RESULT);
    }

    @Test
    void dispatchSwallowsHandlerExceptions() {
        NotificationResultHandler handler = mock(NotificationResultHandler.class);
        when(registry.getHandler("h-1")).thenReturn(handler);
        doThrow(new RuntimeException("handler boom")).when(handler).handleResult(RESULT);

        assertThatCode(() -> new ResultDispatcher(registry).dispatch(RESULT))
            .doesNotThrowAnyException();
    }

    @Test
    void dispatchIsNoOpWhenHandlerMissing() {
        when(registry.getHandler("h-1")).thenReturn(null);

        assertThatCode(() -> new ResultDispatcher(registry).dispatch(RESULT))
            .doesNotThrowAnyException();
    }

    @Test
    void resultEchoesCommandIdentityFields() {
        NotificationCommand cmd = new NotificationCommand(
            "tok", "title", "body", "corr-9", Map.of("k", "v"), "h-9");

        NotificationResult result = ResultDispatcher.result(
            NotificationOutcome.FAILED, cmd, "ticket-9", "boom");

        assertThat(result.outcome()).isEqualTo(NotificationOutcome.FAILED);
        assertThat(result.handlerId()).isEqualTo("h-9");
        assertThat(result.correlationId()).isEqualTo("corr-9");
        assertThat(result.pushToken()).isEqualTo("tok");
        assertThat(result.title()).isEqualTo("title");
        assertThat(result.body()).isEqualTo("body");
        assertThat(result.ticketId()).isEqualTo("ticket-9");
        assertThat(result.errorDetail()).isEqualTo("boom");
        assertThat(result.metadata()).containsEntry("k", "v");
    }
}
