package dev.expopush.backend.local;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LocalBackendIntegrationTest {

    @Mock private ExpoGateway expoGateway;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler resultHandler;

    private LocalNotificationBackend backend;
    private LocalReceiptOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new LocalReceiptOrchestrator(expoGateway, registry, 3, 100, 100);
        orchestrator.start();
        
        backend = new LocalNotificationBackend(expoGateway, orchestrator, registry, Runnable::run, 100);
        
        lenient().when(registry.getHandler(anyString())).thenReturn(resultHandler);
    }

    @Test
    void endToEndLocalDelivery() {
        // Arrange
        NotificationCommand cmd = new NotificationCommand(
            "token", "title", "body", "corr", Map.of(), "handler"
        );
        
        PushTicket ticket = new PushTicket();
        ticket.setStatus(PushTicket.StatusEnum.OK);
        ticket.setId("t-123");
        PushTicketResponse ticketResponse = new PushTicketResponse();
        ticketResponse.setData(List.of(ticket));
        
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse);
        
        PushReceipt receipt = new PushReceipt();
        receipt.setStatus(PushReceipt.StatusEnum.OK);
        PushReceiptResponse receiptResponse = new PushReceiptResponse();
        receiptResponse.setData(Map.of("t-123", receipt));
        
        when(expoGateway.getReceipts(List.of("t-123"))).thenReturn(receiptResponse);

        // Act
        backend.submit(cmd);

        // Assert
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> verify(resultHandler).handleResult(argThat(res ->
            res.outcome() == NotificationOutcome.ACCEPTED && "t-123".equals(res.ticketId()))));
        
        orchestrator.stop();
    }
}
