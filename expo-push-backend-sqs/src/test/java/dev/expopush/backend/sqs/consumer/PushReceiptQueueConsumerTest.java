package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.backend.sqs.message.PushReceiptSqsMessage;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushError;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptDetails;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.NoOpPayloadEncryptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
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
class PushReceiptQueueConsumerTest {

    @Mock private SqsClient sqsClient;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler resultHandler;
    @Mock private ExpoGateway expoGateway;
    @Mock private ExpoRateLimiter rateLimiter;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private PushReceiptQueueConsumer consumer;

    private static final String RECEIPT_QUEUE_URL = "https://sqs.test/receipt";
    private static final int MAX_RECEIPT_ATTEMPTS = 3;

    @BeforeEach
    void setUp() {
        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(GetQueueUrlResponse.builder().queueUrl(RECEIPT_QUEUE_URL).build());

        var config = new PushReceiptQueueConsumer.Config(MAX_RECEIPT_ATTEMPTS, 30_000L, "receipt-queue");
        consumer = new PushReceiptQueueConsumer(
            sqsClient, registry, expoGateway, rateLimiter,
            new NoOpPayloadEncryptor(), objectMapper, config);

        lenient().when(registry.getHandler(any())).thenReturn(resultHandler);
        // Batch deletes return empty-failure responses by default (fully successful call).
        lenient().when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
            .thenReturn(DeleteMessageBatchResponse.builder().build());
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

        verify(expoGateway, after(300).never()).getReceipts(anyList());
        consumer.stop();
        verifyNoInteractions(resultHandler);
    }

    // ─── Poison message ───────────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void poisonMessageIsDeletedNotProcessed() {
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

    // ─── Receipt OK ───────────────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void receiptOkFiresAcceptedAndDeletesMessage() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushReceiptResponse response = receiptResponse("ticket-1", PushReceipt.StatusEnum.OK, null);
        when(expoGateway.getReceipts(anyList())).thenReturn(response);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.ACCEPTED);
            assertThat(cap.getValue().ticketId()).isEqualTo("ticket-1");
            verify(sqsClient, atLeastOnce()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        });
        consumer.stop();
    }

    // ─── Receipt ERROR ────────────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void receiptDeviceNotRegisteredFiresRejected() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushReceiptResponse response = receiptResponse("ticket-1", PushReceipt.StatusEnum.ERROR, "DeviceNotRegistered");
        when(expoGateway.getReceipts(anyList())).thenReturn(response);
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
    void receiptOtherErrorFiresInvalid() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushReceiptResponse response = receiptResponse("ticket-1", PushReceipt.StatusEnum.ERROR, "MessageTooBig");
        when(expoGateway.getReceipts(anyList())).thenReturn(response);
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

    // ─── Receipt not yet available ────────────────────────────────────────────

    @Test
    @Timeout(5)
    void receiptNotFoundBelowThresholdLeavesMessageInQueue() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1); // below maxReceiptAttempts=3

        PushReceiptResponse empty = new PushReceiptResponse();
        when(expoGateway.getReceipts(anyList())).thenReturn(empty);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(expoGateway, atLeastOnce()).getReceipts(anyList()));
        consumer.stop();
        verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verifyNoInteractions(resultHandler);
    }

    @Test
    @Timeout(5)
    void receiptNotFoundAtThresholdFiresUnknownAndDeletes() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), MAX_RECEIPT_ATTEMPTS); // = threshold

        PushReceiptResponse empty = new PushReceiptResponse();
        when(expoGateway.getReceipts(anyList())).thenReturn(empty);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.UNKNOWN);
            verify(sqsClient, atLeastOnce()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        });
        consumer.stop();
    }

    // ─── Batch-level errors ───────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void batchLevelExpoErrorsLeaveMessagesInQueue() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushReceiptResponse response = new PushReceiptResponse();
        PushError err = new PushError();
        err.setCode("INTERNAL_ERROR");
        err.setMessage("Expo internal error");
        response.setErrors(List.of(err));

        when(expoGateway.getReceipts(anyList())).thenReturn(response);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(expoGateway, atLeastOnce()).getReceipts(anyList()));
        consumer.stop();
        verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verifyNoInteractions(resultHandler);
    }

    // ─── Expo exception ───────────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void expoExceptionLeaveMessagesInQueue() throws Exception {
        PushReceiptSqsMessage msg = receiptMsg("ticket-1", "corr-1", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        when(expoGateway.getReceipts(anyList())).thenThrow(new RuntimeException("Expo down"));
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(expoGateway, atLeastOnce()).getReceipts(anyList()));
        consumer.stop();
        verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verifyNoInteractions(resultHandler);
    }

    // ─── Null metadata decryption ─────────────────────────────────────────────

    @Test
    @Timeout(5)
    void nullMetadataHandledGracefully() throws Exception {
        PushReceiptSqsMessage msg = new PushReceiptSqsMessage(
            "ticket-1", "token", "corr-1", null, "h-1", "title", "body");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);

        PushReceiptResponse response = receiptResponse("ticket-1", PushReceipt.StatusEnum.OK, null);
        when(expoGateway.getReceipts(anyList())).thenReturn(response);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.ACCEPTED);
        });
        consumer.stop();
    }

    // ─── Decryption failure ───────────────────────────────────────────────────

    @Test
    @Timeout(5)
    void undecryptableMessageFiresUnknownAndIsDeletedInsteadOfLoopingForever() throws Exception {
        // Expo already accepted this notification (it has a ticket), so an undecryptable
        // receipt message resolves to UNKNOWN — never FAILED — and must not stall the queue.
        dev.expopush.core.security.PayloadEncryptor failingEncryptor = new dev.expopush.core.security.PayloadEncryptor() {
            @Override public String encrypt(String plaintext) { return plaintext; }
            @Override public String decrypt(String ciphertext) { throw new IllegalStateException("bad key"); }
        };
        var config = new PushReceiptQueueConsumer.Config(MAX_RECEIPT_ATTEMPTS, 30_000L, "receipt-queue");
        consumer = new PushReceiptQueueConsumer(
            sqsClient, registry, expoGateway, rateLimiter, failingEncryptor, objectMapper, config);

        PushReceiptSqsMessage msg = receiptMsg("ticket-x", "corr-x", "h-1");
        Message sqsMsg = sqsMessage(objectMapper.writeValueAsString(msg), 1);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
            .thenReturn(ReceiveMessageResponse.builder().messages(List.of(sqsMsg)).build())
            .thenReturn(emptyResponse());
        when(expoGateway.getReceipts(anyList()))
            .thenReturn(receiptResponse("ticket-x", PushReceipt.StatusEnum.OK, null));
        startConsumer();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler, atLeastOnce()).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.UNKNOWN);
            assertThat(cap.getValue().errorDetail()).contains("decryption");
            assertThat(cap.getValue().ticketId()).isEqualTo("ticket-x");
            verify(sqsClient, atLeastOnce()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        });
        consumer.stop();
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

    private static PushReceiptSqsMessage receiptMsg(String ticketId, String correlationId, String handlerId) {
        return new PushReceiptSqsMessage(
            ticketId, "token", correlationId, Map.of(), handlerId, "title", "body");
    }

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
