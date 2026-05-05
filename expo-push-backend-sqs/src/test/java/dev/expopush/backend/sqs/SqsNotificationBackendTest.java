package dev.expopush.backend.sqs;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.core.security.AesPayloadEncryptor;
import dev.expopush.core.security.NoOpPayloadEncryptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.jackson.databind.ObjectMapper;

import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SqsNotificationBackendTest {

    @Mock private SqsClient sqsClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NoOpPayloadEncryptor noOpEncryptor = new NoOpPayloadEncryptor();

    private SqsNotificationBackend backend() {
        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(GetQueueUrlResponse.builder().queueUrl("https://sqs.test/push-queue").build());
        return new SqsNotificationBackend(sqsClient, objectMapper, noOpEncryptor, "push-queue");
    }

    // ─── Happy path ───────────────────────────────────────────────────────────

    @Test
    void submitSendsMessageToCorrectQueue() {
        SqsNotificationBackend b = backend();
        NotificationCommand cmd = new NotificationCommand(
            "token", "title", "body", "corr-1", Map.of(), "h-1");

        b.submit(cmd);

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(cap.capture());
        assertThat(cap.getValue().queueUrl()).isEqualTo("https://sqs.test/push-queue");
    }

    @Test
    void submitSerializesCommandFieldsIntoMessageBody() throws Exception {
        SqsNotificationBackend b = backend();
        NotificationCommand cmd = new NotificationCommand(
            "ExponentPushToken[abc]", "Hello", "World", "corr-42", Map.of("key", "val"), "handler-1");

        b.submit(cmd);

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(cap.capture());
        String body = cap.getValue().messageBody();
        assertThat(body).contains("ExponentPushToken[abc]", "corr-42", "handler-1");
    }

    @Test
    void submitEncryptsTitleAndBody() {
        String key = Base64.getEncoder().encodeToString(new byte[32]);
        AesPayloadEncryptor encryptor = new AesPayloadEncryptor(key);

        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(GetQueueUrlResponse.builder().queueUrl("https://sqs.test/q").build());
        SqsNotificationBackend b = new SqsNotificationBackend(sqsClient, objectMapper, encryptor, "q");

        NotificationCommand cmd = new NotificationCommand(
            "token", "secret title", "secret body", "corr-1", Map.of(), "h-1");

        b.submit(cmd);

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(cap.capture());
        String msgBody = cap.getValue().messageBody();
        assertThat(msgBody)
            .doesNotContain("secret title")
            .doesNotContain("secret body");
    }

    @Test
    void submitWithNullMetadataDoesNotThrow() {
        SqsNotificationBackend b = backend();
        NotificationCommand cmd = new NotificationCommand(
            "token", "title", "body", "corr-1", null, "h-1");

        b.submit(cmd); // should not throw

        verify(sqsClient).sendMessage(any(SendMessageRequest.class));
    }

    @Test
    void submitWithNonNullMetadataEncryptsValues() {
        SqsNotificationBackend b = backend();
        NotificationCommand cmd = new NotificationCommand(
            "token", "title", "body", "corr-1", Map.of("k1", "v1", "k2", "v2"), "h-1");

        b.submit(cmd);

        ArgumentCaptor<SendMessageRequest> cap = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(sqsClient).sendMessage(cap.capture());
        // NoOp encryptor - values pass through unchanged
        assertThat(cap.getValue().messageBody()).contains("v1").contains("v2");
    }

    // ─── Failure path ─────────────────────────────────────────────────────────

    @Test
    void sqsExceptionThrowsNotificationSubmissionException() {
        SqsNotificationBackend b = backend();
        when(sqsClient.sendMessage(any(SendMessageRequest.class)))
            .thenThrow(new RuntimeException("SQS down"));

        NotificationCommand cmd = new NotificationCommand(
            "token", "title", "body", "corr-1", Map.of(), "h-1");

        assertThatThrownBy(() -> b.submit(cmd))
            .isInstanceOf(NotificationSubmissionException.class)
            .hasMessageContaining("corr-1");
    }

    @Test
    void sqsConstructorResolvesQueueUrl() {
        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class)))
            .thenReturn(GetQueueUrlResponse.builder().queueUrl("https://sqs.test/resolved").build());

        new SqsNotificationBackend(sqsClient, objectMapper, noOpEncryptor, "my-queue");

        ArgumentCaptor<GetQueueUrlRequest> cap = ArgumentCaptor.forClass(GetQueueUrlRequest.class);
        verify(sqsClient).getQueueUrl(cap.capture());
        assertThat(cap.getValue().queueName()).isEqualTo("my-queue");
    }
}
