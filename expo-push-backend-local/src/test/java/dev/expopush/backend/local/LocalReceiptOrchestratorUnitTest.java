package dev.expopush.backend.local;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptDetails;
import dev.expopush.core.api.model.PushReceiptResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LocalReceiptOrchestratorUnitTest {

    @Mock private ExpoGateway expoGateway;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler handler;

    private LocalReceiptOrchestrator orchestrator;

    private static final NotificationCommand CMD = new NotificationCommand(
        "token", "Title", "Body", "corr-1", Map.of(), "h-1");

    @BeforeEach
    void setUp() {
        orchestrator = new LocalReceiptOrchestrator(expoGateway, registry, 3, 30_000L, 100);
        orchestrator.start();
        lenient().when(registry.getHandler("h-1")).thenReturn(handler);
    }

    @AfterEach
    void tearDown() {
        orchestrator.stop();
    }

    // ─── submitTask ───────────────────────────────────────────────────────────

    @Test
    void submitTaskReturnsTrueWhenQueueHasSpace() {
        LocalReceiptOrchestrator small = new LocalReceiptOrchestrator(expoGateway, registry, 3, 60_000L, 10);
        assertThat(small.submitTask(new DelayedReceiptTask("t-1", CMD, 60_000L, 1))).isTrue();
    }

    @Test
    void submitTaskReturnsFalseWhenQueueFull() {
        LocalReceiptOrchestrator full = new LocalReceiptOrchestrator(expoGateway, registry, 3, 60_000L, 0);
        assertThat(full.submitTask(new DelayedReceiptTask("t-1", CMD, 60L, 1))).isFalse();
    }

    // ─── processTask: receipt OK ──────────────────────────────────────────────

    @Test
    @Timeout(5)
    void receiptOkFiresAccepted() {
        PushReceiptResponse response = receiptResponse("t-1", PushReceipt.StatusEnum.OK, null);
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(handler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.ACCEPTED);
            assertThat(cap.getValue().ticketId()).isEqualTo("t-1");
        });
    }

    // ─── processTask: receipt ERROR ───────────────────────────────────────────

    @Test
    @Timeout(5)
    void receiptDeviceNotRegisteredFiresRejected() {
        PushReceiptResponse response = receiptResponse("t-1", PushReceipt.StatusEnum.ERROR, "DeviceNotRegistered");
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(handler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.REJECTED);
        });
    }

    @Test
    @Timeout(5)
    void receiptOtherErrorFiresInvalid() {
        PushReceiptResponse response = receiptResponse("t-1", PushReceipt.StatusEnum.ERROR, "MessageTooBig");
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(handler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.INVALID);
        });
    }

    @Test
    @Timeout(5)
    void receiptErrorWithNullDetailsUsesMessage() {
        PushReceipt receipt = new PushReceipt();
        receipt.setStatus(PushReceipt.StatusEnum.ERROR);
        receipt.setMessage("some error");
        PushReceiptResponse response = new PushReceiptResponse();
        response.setData(Map.of("t-1", receipt));
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(handler).handleResult(cap.capture());
            assertThat(cap.getValue().errorDetail()).isEqualTo("some error");
        });
    }

    // ─── processTask: missing receipt ────────────────────────────────────────

    @Test
    @Timeout(5)
    void missingReceiptBeforeMaxAttemptsIsRetried() {
        // Return empty response — receipt not yet available
        PushReceiptResponse empty = new PushReceiptResponse();
        empty.setData(Map.of());
        when(expoGateway.getReceipts(anyList())).thenReturn(empty);

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        // Should retry — handler NOT called yet on first attempt
        await().during(200, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.SECONDS)
            .until(() -> true); // just wait a bit
        verify(handler, never()).handleResult(any());
    }

    @Test
    @Timeout(5)
    void maxAttemptsReachedFiresUnknown() {
        PushReceiptResponse empty = new PushReceiptResponse();
        empty.setData(Map.of());
        when(expoGateway.getReceipts(anyList())).thenReturn(empty);

        // Submit at max attempts (3 = maxAttempts configured in setUp)
        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 3));

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(handler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.UNKNOWN);
        });
    }

    @Test
    @Timeout(5)
    void expoExceptionOnReceiptFetchRetriesTask() {
        when(expoGateway.getReceipts(anyList())).thenThrow(new RuntimeException("Network error"));

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        // Not called immediately — will retry
        await().during(100, TimeUnit.MILLISECONDS).atMost(500, TimeUnit.SECONDS).until(() -> true);
        verify(handler, never()).handleResult(any());
    }

    // ─── Lifecycle ────────────────────────────────────────────────────────────

    @Test
    void stopSignalsWorkerToExit() {
        LocalReceiptOrchestrator orc = new LocalReceiptOrchestrator(expoGateway, registry, 3, 1000L, 10);
        orc.start();
        assertThatCode(orc::stop).doesNotThrowAnyException();
    }

    // ─── Handler null-safety ──────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void missingHandlerDoesNotThrow() {
        when(registry.getHandler("h-1")).thenReturn(null);
        PushReceiptResponse response = receiptResponse("t-1", PushReceipt.StatusEnum.OK, null);
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        orchestrator.submitTask(new DelayedReceiptTask("t-1", CMD, 0L, 1));

        // Should not crash — just log and move on
        await().atMost(2, TimeUnit.SECONDS)
            .until(() -> mockingDetails(expoGateway).getInvocations().stream()
                .anyMatch(inv -> inv.getMethod().getName().equals("getReceipts")));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static PushReceiptResponse receiptResponse(String ticketId, PushReceipt.StatusEnum status, String errorCode) {
        PushReceipt receipt = new PushReceipt();
        receipt.setStatus(status);
        if (errorCode != null) {
            PushReceiptDetails details = new PushReceiptDetails();
            details.setError(errorCode);
            receipt.setDetails(details);
        }
        PushReceiptResponse response = new PushReceiptResponse();
        response.setData(Map.of(ticketId, receipt));
        return response;
    }
}
