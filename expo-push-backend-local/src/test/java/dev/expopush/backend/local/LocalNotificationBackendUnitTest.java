package dev.expopush.backend.local;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LocalNotificationBackendUnitTest {

    @Mock private ExpoGateway expoGateway;
    @Mock private LocalReceiptOrchestrator orchestrator;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler handler;

    private LocalNotificationBackend backend;

    private static final NotificationCommand CMD = new NotificationCommand(
        "ExponentPushToken[test]", "Title", "Body", "corr-1", Map.of(), "h-1");

    @BeforeEach
    void setUp() {
        // Direct executor keeps the async hand-off synchronous for deterministic tests.
        backend = new LocalNotificationBackend(expoGateway, orchestrator, registry, Runnable::run, 100L);
        lenient().when(registry.getHandler("h-1")).thenReturn(handler);
    }

    // ─── Null / error Expo response ───────────────────────────────────────────

    @Test
    void nullExpoResponseFiresUnknown() {
        // A null/empty 200 response is ambiguous — Expo may have processed the request,
        // so the outcome is UNKNOWN (duplicate risk), not FAILED (safe to retry).
        when(expoGateway.sendNotifications(anyList())).thenReturn(null);

        backend.submit(CMD);

        verifyHandlerCalledWith(NotificationOutcome.UNKNOWN, null);
        verify(orchestrator, never()).submitTask(any());
    }

    @Test
    void expoResponseWithOnlyErrorsFiresFailed() {
        PushTicketResponse response = new PushTicketResponse();
        PushError error = new PushError();
        error.setMessage("Rate limit hit");
        response.setErrors(List.of(error));

        when(expoGateway.sendNotifications(anyList())).thenReturn(response);

        backend.submit(CMD);

        ArgumentCaptor<NotificationResult> captor = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(captor.capture());
        assertThat(captor.getValue().outcome()).isEqualTo(NotificationOutcome.FAILED);
        assertThat(captor.getValue().errorDetail()).isEqualTo("Rate limit hit");
    }

    // ─── Ticket OK ────────────────────────────────────────────────────────────

    @Test
    void ticketOkSubmitsReceiptTask() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.OK, "ticket-42", null);
        PushTicketResponse response = ticketResponse(ticket);
        when(expoGateway.sendNotifications(anyList())).thenReturn(response);
        when(orchestrator.submitTask(any())).thenReturn(true);

        backend.submit(CMD);

        ArgumentCaptor<DelayedReceiptTask> captor = ArgumentCaptor.forClass(DelayedReceiptTask.class);
        verify(orchestrator).submitTask(captor.capture());
        assertThat(captor.getValue().getTicketId()).isEqualTo("ticket-42");
        assertThat(captor.getValue().getAttempt()).isEqualTo(1);
    }

    @Test
    void ticketOkQueueFullFiresUnknown() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.OK, "ticket-42", null);
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));
        when(orchestrator.submitTask(any())).thenReturn(false);

        backend.submit(CMD);

        verifyHandlerCalledWith(NotificationOutcome.UNKNOWN, "ticket-42");
    }

    // ─── Ticket ERROR ─────────────────────────────────────────────────────────

    @Test
    void ticketDeviceNotRegisteredFiresRejected() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "DeviceNotRegistered");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        verifyHandlerCalledWith(NotificationOutcome.REJECTED, null);
    }

    @Test
    void ticketOtherErrorFiresInvalid() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "MessageTooBig");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        verifyHandlerCalledWith(NotificationOutcome.INVALID, null);
    }

    @Test
    void ticketErrorWithNullDetailsUsesMessage() {
        PushTicket ticket = new PushTicket();
        ticket.setStatus(PushTicket.StatusEnum.ERROR);
        ticket.setMessage("some error message");
        // getDetails() returns null
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        ArgumentCaptor<NotificationResult> captor = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(captor.capture());
        assertThat(captor.getValue().errorDetail()).isEqualTo("some error message");
    }

    // ─── Expo exception ───────────────────────────────────────────────────────

    @Test
    void expoExceptionFiresFailedInsteadOfThrowing() {
        // The send runs off the caller's thread, so failures surface via the handler.
        when(expoGateway.sendNotifications(anyList())).thenThrow(new RuntimeException("Expo down"));

        assertThatCode(() -> backend.submit(CMD)).doesNotThrowAnyException();

        ArgumentCaptor<NotificationResult> captor = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(captor.capture());
        assertThat(captor.getValue().outcome()).isEqualTo(NotificationOutcome.FAILED);
        assertThat(captor.getValue().errorDetail()).isEqualTo("Expo down");
    }

    @Test
    void executorRejectionThrowsSubmissionException() {
        LocalNotificationBackend rejecting = new LocalNotificationBackend(
            expoGateway, orchestrator, registry,
            task -> { throw new java.util.concurrent.RejectedExecutionException("shutting down"); },
            100L);

        assertThatThrownBy(() -> rejecting.submit(CMD))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("rejected");

        verifyNoInteractions(expoGateway);
    }

    // ─── Handler null-safety ─────────────────────────────────────────────────

    @Test
    void missingHandlerDoesNotThrow() {
        when(registry.getHandler("h-1")).thenReturn(null);
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "DeviceNotRegistered");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        assertThatCode(() -> backend.submit(CMD)).doesNotThrowAnyException();
    }

    @Test
    void handlerExceptionIsSwallowed() {
        doThrow(new RuntimeException("handler exploded")).when(handler).handleResult(any());
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "DeviceNotRegistered");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        assertThatCode(() -> backend.submit(CMD)).doesNotThrowAnyException();
    }

    // ─── Delivery options plumb-through ───────────────────────────────────────

    @Test
    void deliveryOptionsReachTheExpoRequest() {
        NotificationCommand cmd = new NotificationCommand(
            "tok", "t", "b", "corr-1", Map.of(), "h-1",
            new dev.expopush.api.NotificationOptions(
                Map.of("screen", "orders"), "order-updates", "default", 3600, 2, "Shipped",
                dev.expopush.api.NotificationPriority.HIGH));
        PushTicket ticket = ticket(PushTicket.StatusEnum.OK, "ticket-1", null);
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));
        when(orchestrator.submitTask(any())).thenReturn(true);

        backend.submit(cmd);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<dev.expopush.core.api.model.PushMessage>> cap =
            ArgumentCaptor.forClass((Class) List.class);
        verify(expoGateway).sendNotifications(cap.capture());
        dev.expopush.core.api.model.PushMessage pm = cap.getValue().getFirst();
        assertThat(pm.getData()).containsEntry("screen", "orders");
        assertThat(pm.getChannelId()).isEqualTo("order-updates");
        assertThat(pm.getSubtitle()).isEqualTo("Shipped");
        assertThat(pm.getTtl()).isEqualTo(3600);
        assertThat(pm.getBadge()).isEqualTo(2);
        assertThat(pm.getPriority()).isEqualTo(dev.expopush.core.api.model.PushMessage.PriorityEnum.HIGH);
    }

    // ─── Result field mapping ─────────────────────────────────────────────────

    @Test
    void resultFieldsMappedFromCommand() {
        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "MessageTooBig");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));

        backend.submit(CMD);

        ArgumentCaptor<NotificationResult> captor = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(captor.capture());
        NotificationResult result = captor.getValue();
        assertThat(result.handlerId()).isEqualTo("h-1");
        assertThat(result.correlationId()).isEqualTo("corr-1");
        assertThat(result.pushToken()).isEqualTo("ExponentPushToken[test]");
        assertThat(result.title()).isEqualTo("Title");
        assertThat(result.body()).isEqualTo("Body");
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private void verifyHandlerCalledWith(NotificationOutcome expectedOutcome, String expectedTicketId) {
        ArgumentCaptor<NotificationResult> captor = ArgumentCaptor.forClass(NotificationResult.class);
        verify(handler).handleResult(captor.capture());
        assertThat(captor.getValue().outcome()).isEqualTo(expectedOutcome);
        assertThat(captor.getValue().ticketId()).isEqualTo(expectedTicketId);
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
