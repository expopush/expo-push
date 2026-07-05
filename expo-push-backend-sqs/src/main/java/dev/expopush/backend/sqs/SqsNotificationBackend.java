package dev.expopush.backend.sqs;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.sqs.consumer.PushNotificationQueueConsumer;
import dev.expopush.backend.sqs.consumer.PushReceiptQueueConsumer;
import dev.expopush.backend.sqs.message.PushNotificationSqsMessage;
import dev.expopush.core.security.PayloadEncryptor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.jackson.databind.ObjectMapper;

/**
 * SQS-backed {@link NotificationBackend} that enqueues submitted commands onto the
 * Push Notification Queue for asynchronous delivery.
 *
 * <p>The consumers ({@link PushNotificationQueueConsumer}
 * and {@link PushReceiptQueueConsumer}) are registered
 * as separate Spring beans by the auto-configuration and manage their own lifecycle via
 * {@link org.springframework.context.SmartLifecycle}.
 */
@Slf4j
public class SqsNotificationBackend implements NotificationBackend {

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final PayloadEncryptor encryptor;
    private final String pushQueueUrl;

    public SqsNotificationBackend(
            SqsClient sqsClient, 
            ObjectMapper objectMapper, 
            PayloadEncryptor encryptor,
            String pushQueueName) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.encryptor = encryptor;
        this.pushQueueUrl = sqsClient.getQueueUrl(
            GetQueueUrlRequest.builder().queueName(pushQueueName).build()
        ).queueUrl();
    }

    @Override
    public void submit(NotificationCommand command) {
        try {
            dev.expopush.api.NotificationOptions opts = command.options();
            // data and subtitle are content — encrypted at rest like title/body. The
            // remaining option fields are delivery mechanics and stay plaintext.
            String dataJson = (opts == null || opts.data() == null)
                ? null
                : encryptor.encrypt(objectMapper.writeValueAsString(opts.data()));
            PushNotificationSqsMessage message = new PushNotificationSqsMessage(
                command.pushToken(),
                encryptor.encrypt(command.title()),
                encryptor.encrypt(command.body()),
                command.correlationId(),
                encryptMap(command.metadata()),
                command.handlerId(),
                dev.expopush.backend.sqs.message.SqsNotificationMessage.CURRENT_SCHEMA_VERSION,
                dataJson,
                opts == null ? null : encryptor.encrypt(opts.subtitle()),
                opts == null ? null : opts.channelId(),
                opts == null ? null : opts.sound(),
                opts == null ? null : opts.ttl(),
                opts == null ? null : opts.badge(),
                opts == null || opts.priority() == null ? null : opts.priority().name()
            );
            String body = objectMapper.writeValueAsString(message);
            sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(pushQueueUrl)
                .messageBody(body)
                .build());
            log.debug("Enqueued notification command for correlationId={} handlerId={}",
                command.correlationId(), command.handlerId());
        } catch (Exception e) {
            throw new NotificationSubmissionException(
                "Failed to enqueue notification command for correlationId=" + command.correlationId()
                    + " onto " + pushQueueUrl, e);
        }
    }

    private java.util.Map<String, String> encryptMap(java.util.Map<String, String> map) {
        if (map == null) return java.util.Map.of();
        return map.entrySet().stream()
            .collect(java.util.stream.Collectors.toMap(
                java.util.Map.Entry::getKey,
                e -> encryptor.encrypt(e.getValue())
            ));
    }
}
