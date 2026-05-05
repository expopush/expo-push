package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.backend.sqs.message.PushNotificationSqsMessage;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushError;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketDetails;
import dev.expopush.core.api.model.PushTicketResponse;
import dev.expopush.core.exception.ExpoAuthException;
import dev.expopush.core.exception.ExpoRateLimitException;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.NoOpPayloadEncryptor;
import io.github.resilience4j.retry.Retry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PushNotificationQueueConsumerTest {

    @Mock private SqsClient sqsClient;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler resultHandler;
    @Mock private ExpoGateway expoGateway;
    @Mock private ExpoRateLimiter rateLimiter;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private PushNotificationQueueConsumer consumer;

    private static final String PUSH_QUEUE_URL = "https://sqs.test/push";
    private static final String RECEIPT_QUEUE_URL = "https://sqs.test/receipt";

    @BeforeEach
    void setUp() {
        lenient().when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenAnswer(inv -> {
                String name = ((GetQueueUrlRequest) inv.getArgument(0)).queueName();
                String url = name.contains("receipt") ? RECEIPT_QUEUE_URL : PUSH_QUEUE_URL;
                return GetQueueUrlResponse.builder().queueUrl(url).build();
            });

        var config = new PushNotificationQueueConsumer.Config(
            Retry.ofDefaults("test"), 10, 900, 3, 5, 30_000L, "push-queue", "receipt-queue");
        consumer = new PushNotificationQueueConsumer(
            sqsClient, registry, expoGateway, rateLimiter, new NoOpPayloadEncryptor(), objectMapper, config);

        lenient().when(registry.getHandler(any())).thenReturn(resultHandler);
    }

    /** Call after test-specific mocks are set up — avoids race between consumer thread and stub setup. */
    private void startConsumer() {
        consumer.start();
    }

    @AfterEach
    void tearDown() {
        if (consumer.isRunning()) consumer.stop();
    }

    // ─── Empty queue ──────────────────────────────────────────────────────────

    @Test
    @Timeout(3)
    void emptyQueueResponseDoesNothing() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of()).build());
        startConsumer();

        verify(expoGateway, after(300).never()).sendNotifications(anyList());
        consumer.stop();
        verifyNoInteractions(resultHandler);
    }

    // ─── Poison message deserialization ──────────────────────────────────────

    @Test
    @Timeout(5)
    void poisonMessageIsDeletedNotProcessed() throws Exception {
        Message poisonMsg = sqsMessage("not-valid-json", 1);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(poisonMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(sqsClient, atLeastOnce()).deleteMessage(any(DeleteMessageRequest.class)));
        consumer.stop();
        verifyNoInteractions(expoGateway);
    }

    // ─── Successful delivery: ticket OK ──────────────────────────────────────

    @Test
    @Timeout(5)
    void ticketOkPostsToReceiptQueueAndDeletesFromPushQueue() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushTicket ticket = ticket(PushTicket.StatusEnum.OK, "ticket-1", null);
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(sqsClient, atLeastOnce()).sendMessage(any(SendMessageRequest.class)));
        consumer.stop();

        ArgumentCaptor<SendMessageRequest> sendCap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient, atLeastOnce()).sendMessage(sendCap.capture());
        assertThat(sendCap.getValue().queueUrl()).isEqualTo(RECEIPT_QUEUE_URL);
        assertThat(sendCap.getValue().messageBody()).contains("ticket-1");
        verify(sqsClient, atLeastOnce()).deleteMessage(any(DeleteMessageRequest.class));
    }

    // ─── Ticket ERROR ─────────────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void ticketDeviceNotRegisteredFiresRejected() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "DeviceNotRegistered");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.REJECTED);
        });
        consumer.stop();
    }

    @Test
    @Timeout(5)
    void ticketOtherErrorFiresInvalid() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushTicket ticket = ticket(PushTicket.StatusEnum.ERROR, null, "MessageTooBig");
        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse(ticket));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.INVALID);
        });
        consumer.stop();
    }

    // ─── Batch-level errors ───────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void batchLevelErrorFiresInvalidForAllMessages() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushTicketResponse response = new PushTicketResponse();
        PushError err = new PushError();
        err.setCode("PUSH_TOO_MANY_EXPERIENCE_IDS");
        err.setMessage("Too many experience IDs");
        response.setErrors(List.of(err));

        when(expoGateway.sendNotifications(anyList())).thenReturn(response);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.INVALID);
        });
        consumer.stop();
    }

    // ─── ExpoAuthException halts consumer ────────────────────────────────────

    @Test
    @Timeout(5)
    void expoAuthExceptionHaltsConsumer() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        when(expoGateway.sendNotifications(anyList())).thenThrow(new ExpoAuthException("Bad token"));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !consumer.isRunning());
        assertThat(consumer.isRunning()).isFalse();
    }

    // ─── Retryable exhausted: receive count logic ─────────────────────────────

    @Test
    @Timeout(5)
    void retryableExhaustedBelowMaxReceivesLeavesMessageInQueue() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 2); // below maxPushRetryReceives=5

        when(expoGateway.sendNotifications(anyList()))
            .thenThrow(new ExpoRateLimitException("Rate limited"));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(expoGateway, atLeastOnce()).sendNotifications(anyList()));
        consumer.stop();
        verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
        verifyNoInteractions(resultHandler);
    }

    @Test
    @Timeout(5)
    void retryableExhaustedAtMaxReceivesFiresFailedAndDeletes() throws Exception {
        PushNotificationSqsMessage msg = pushMsg("corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 5); // = maxPushRetryReceives

        when(expoGateway.sendNotifications(anyList()))
            .thenThrow(new ExpoRateLimitException("Rate limited"));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.FAILED);
            verify(sqsClient, atLeastOnce()).deleteMessage(any(DeleteMessageRequest.class));
        });
        consumer.stop();
    }

    // ─── parseReceiveCount ────────────────────────────────────────────────────

    @Test
    void parseReceiveCountReturnsAttributeValue() {
        Message msg = Message.builder()
            .attributes(Map.of(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "7"))
            .build();
        assertThat(PushNotificationQueueConsumer.parseReceiveCount(msg)).isEqualTo(7);
    }

    @Test
    void parseReceiveCountDefaultsToOneWhenAbsent() {
        Message msg = Message.builder().attributes(Map.of()).build();
        assertThat(PushNotificationQueueConsumer.parseReceiveCount(msg)).isEqualTo(1);
    }

    @Test
    void parseReceiveCountDefaultsToOneOnParseError() {
        Message msg = Message.builder()
            .attributes(Map.of(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "not-a-number"))
            .build();
        assertThat(PushNotificationQueueConsumer.parseReceiveCount(msg)).isEqualTo(1);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static Message sqsMessage(String body, int receiveCount) {
        return Message.builder()
            .body(body)
            .receiptHandle("rh-" + body.hashCode())
            .attributes(Map.of(
                MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, String.valueOf(receiveCount)))
            .build();
    }

    private static ReceiveMessageResponse emptyResponse() {
        return ReceiveMessageResponse.builder().messages(List.of()).build();
    }

    private static PushNotificationSqsMessage pushMsg(String correlationId, String handlerId) {
        return new PushNotificationSqsMessage(
            "token", "title", "body", correlationId, Map.of(), handlerId);
    }

    private static PushTicket ticket(PushTicket.StatusEnum status, String id, String errorCode) {
        PushTicket t = new PushTicket();
        t.setStatus(status);
        t.setId(id);
        if (errorCode != null) {
            PushTicketDetails d = new PushTicketDetails();
            d.setError(errorCode);
            t.setDetails(d);
        }
        return t;
    }

    private static PushTicketResponse ticketResponse(PushTicket ticket) {
        PushTicketResponse r = new PushTicketResponse();
        r.setData(List.of(ticket));
        return r;
    }
}
