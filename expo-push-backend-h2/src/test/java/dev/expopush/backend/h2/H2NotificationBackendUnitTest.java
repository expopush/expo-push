package dev.expopush.backend.h2;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushError;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketDetails;
import dev.expopush.core.api.model.PushTicketResponse;
import dev.expopush.core.security.NoOpPayloadEncryptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class H2NotificationBackendUnitTest {

    @Mock private ExpoGateway expoGateway;
    @Mock private JdbcTemplate jdbcTemplate;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler handler;

    private H2NotificationBackend backend;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final NotificationCommand CMD = new NotificationCommand(
        "ExponentPushToken[test]", "Title", "Body", "corr-1", Map.of(), "h-1");

    @BeforeEach
    void setUp() {
        // Direct executor keeps the async hand-off synchronous for deterministic tests.
        backend = new H2NotificationBackend(
            expoGateway, jdbcTemplate, registry, objectMapper, new NoOpPayloadEncryptor(), Runnable::run, 1L);
        lenient().when(registry.getHandler("h-1")).thenReturn(handler);
    }

    // ─── Outbox write ─────────────────────────────────────────────────────────

    @Test
    void outboxRowInsertedBeforeExpoCall() {
        when(expoGateway.sendNotifications(anyList()))
            .thenAnswer(inv -> { verify(jdbcTemplate).update(contains("INSERT"), any(), any(), any(), any()); return null; });

        try { backend.submit(CMD); } catch (Exception ignored) { /* downstream failures irrelevant to this test */ }
    }

    @Test
    void insertFailureThrowsSubmissionExceptionWithoutCallingExpo() {
        when(jdbcTemplate.update(contains("INSERT"), any(), any(), any(), any()))
            .thenThrow(new RuntimeException("DB down"));

        assertThatThrownBy(() -> backend.submit(CMD))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("outbox record");

        verifyNoInteractions(expoGateway);
    }

    @Test
    void executorRejectionDeletesRowAndThrowsSubmissionException() {
        H2NotificationBackend rejecting = new H2NotificationBackend(
            expoGateway, jdbcTemplate, registry, objectMapper, new NoOpPayloadEncryptor(),
            task -> { throw new java.util.concurrent.RejectedExecutionException("shutting down"); },
            1L);

        assertThatThrownBy(() -> rejecting.submit(CMD))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("rejected");

        verify(jdbcTemplate).update(contains("DELETE"), anyString());
        verifyNoInteractions(expoGateway);
    }

    // ─── Ticket OK ────────────────────────────────────────────────────────────

    @Test
    void ticketOkActivatesPendingRow() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.OK, "ticket-99", null);
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        verify(jdbcTemplate).update(contains("UPDATE"), eq("ticket-99"), any(), anyString());
        verify(handler, never()).handleResult(any());
    }

    // ─── Batch-level error response ───────────────────────────────────────────

    @Test
    void nullExpoResponseDeletesRowAndFiresUnknown() {
        // A null/empty 200 response is ambiguous — Expo may have processed the request,
        // so the outcome is UNKNOWN (duplicate risk), not FAILED (safe to retry).
        when(expoGateway.sendNotifications(anyList())).thenReturn(null);

        backend.submit(CMD);

        verify(jdbcTemplate).update(contains("DELETE"), anyString());
        verifyHandlerCalledWith(NotificationOutcome.UNKNOWN);
    }

    @Test
    void expoResponseWithOnlyErrorsDeletesRowAndFiresFailed() {
        PushTicketResponse response = new PushTicketResponse();
        PushError error = new PushError();
        error.setMessage("Bad request");
        response.setErrors(List.of(error));
        when(expoGateway.sendNotifications(anyList())).thenReturn(response);

        backend.submit(CMD);

        verify(jdbcTemplate).update(contains("DELETE"), anyString());
        ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(cap.capture());
        assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.FAILED);
        assertThat(cap.getValue().errorDetail()).isEqualTo("Bad request");
    }

    // ─── Ticket ERROR ─────────────────────────────────────────────────────────

    @Test
    void ticketDeviceNotRegisteredDeletesRowAndFiresRejected() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "DeviceNotRegistered");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        verify(jdbcTemplate).update(contains("DELETE"), anyString());
        verifyHandlerCalledWith(NotificationOutcome.REJECTED);
    }

    @Test
    void ticketOtherErrorDeletesRowAndFiresInvalid() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "MessageTooBig");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        verify(jdbcTemplate).update(contains("DELETE"), anyString());
        verifyHandlerCalledWith(NotificationOutcome.INVALID);
    }

    @Test
    void ticketErrorWithNullDetailsUsesMessage() {
        PushTicket ticket = new PushTicket();
        ticket.setStatus(PushTicket.StatusEnum.ERROR);
        ticket.setMessage("some error");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(cap.capture());
        assertThat(cap.getValue().errorDetail()).isEqualTo("some error");
    }

    // ─── Expo exception ───────────────────────────────────────────────────────

    @Test
    void expoExceptionDeletesRowAndFiresFailed() {
        // The send runs off the caller's thread, so failures surface via the handler.
        when(expoGateway.sendNotifications(anyList())).thenThrow(new RuntimeException("Expo 500"));

        assertThatCode(() -> backend.submit(CMD)).doesNotThrowAnyException();

        verify(jdbcTemplate).update(contains("DELETE"), anyString());
        ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(cap.capture());
        assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.FAILED);
        assertThat(cap.getValue().errorDetail()).isEqualTo("Expo 500");
    }

    // ─── Handler null-safety ──────────────────────────────────────────────────

    @Test
    void missingHandlerDoesNotThrow() {
        when(registry.getHandler("h-1")).thenReturn(null);
        when(expoGateway.sendNotifications(anyList())).thenReturn(null);

        assertThatCode(() -> backend.submit(CMD)).doesNotThrowAnyException();
    }

    @Test
    void handlerExceptionIsSwallowed() {
        doThrow(new RuntimeException("handler boom")).when(handler).handleResult(any());
        when(expoGateway.sendNotifications(anyList())).thenReturn(null);

        assertThatCode(() -> backend.submit(CMD)).doesNotThrowAnyException();
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private void verifyHandlerCalledWith(NotificationOutcome outcome) {
        ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(cap.capture());
        assertThat(cap.getValue().outcome()).isEqualTo(outcome);
    }

    private static PushTicket ticket(PushTicket.StatusEnum status, String id, String errorCode) {
        PushTicket t = new PushTicket();
        t.setStatus(status);
        t.setId(id);
        if (errorCode != null) {
            PushTicketDetails details = new PushTicketDetails();
            details.setError(errorCode);
            t.setDetails(details);
        }
        return t;
    }

    private static PushTicketResponse ticketResponse(PushTicket ticket) {
        PushTicketResponse r = new PushTicketResponse();
        r.setData(List.of(ticket));
        return r;
    }
}
