package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.backend.sqs.message.SqsNotificationMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AbstractSqsConsumerTest {

    @Mock private SqsClient sqsClient;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler resultHandler;

    private static class TestConsumer extends AbstractSqsConsumer {
        protected TestConsumer(SqsClient sqsClient, NotificationHandlerRegistry registry) {
            super(sqsClient, registry, "test-consumer", 30000);
        }
        @Override protected void processOneBatch() throws InterruptedException {
            // No-op for direct helper testing
        }
    }

    @Test
    void notifyHandlerHandlesHandlerException() {
        TestConsumer consumer = new TestConsumer(sqsClient, registry);
        NotificationResult res = new NotificationResult(
            NotificationOutcome.ACCEPTED, "h-1", "c-1", "t", "T", "B", "tic", null, Map.of()
        );
        when(registry.getHandler("h-1")).thenReturn(resultHandler);
        doThrow(new RuntimeException("Handler Boom")).when(resultHandler).handleResult(res);

        // Should not throw
        consumer.notifyHandler(res);

        verify(resultHandler).handleResult(res);
    }

    @Test
    void deleteMessageHandlesSqsException() {
        TestConsumer consumer = new TestConsumer(sqsClient, registry);
        when(sqsClient.deleteMessage(any(DeleteMessageRequest.class)))
            .thenThrow(new RuntimeException("SQS Boom"));

        // Should not throw
        consumer.deleteMessage("url", "handle");

        verify(sqsClient).deleteMessage(any(DeleteMessageRequest.class));
    }
    
    @Test
    void resultBuildsCorrectObject() {
        TestConsumer consumer = new TestConsumer(sqsClient, registry);
        SqsNotificationMessage msg = mock(SqsNotificationMessage.class);
        when(msg.handlerId()).thenReturn("h-1");
        when(msg.correlationId()).thenReturn("c-1");
        when(msg.pushToken()).thenReturn("token");
        when(msg.metadata()).thenReturn(Map.of("k", "v"));
        
        NotificationResult res = consumer.result(NotificationOutcome.REJECTED, msg, "ticket", "error");
        
        assertThat(res.outcome()).isEqualTo(NotificationOutcome.REJECTED);
        assertThat(res.handlerId()).isEqualTo("h-1");
        assertThat(res.correlationId()).isEqualTo("c-1");
        assertThat(res.pushToken()).isEqualTo("token");
        assertThat(res.ticketId()).isEqualTo("ticket");
        assertThat(res.errorDetail()).isEqualTo("error");
        assertThat(res.metadata()).containsEntry("k", "v");
    }
}
