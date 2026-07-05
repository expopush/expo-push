package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.backend.sqs.message.PushReceiptSqsMessage;
import dev.expopush.backend.sqs.message.SqsNotificationMessage;
import dev.expopush.core.ExpoErrors;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.exception.ExpoAuthException;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.PayloadEncryptor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Consumes the Receipt Queue (RQ), fetches delivery receipts from Expo, and routes
 * the final outcome to the registered handler.
 *
 * <p>Receipt-level outcome mapping:
 * <ul>
 *   <li>Receipt OK → {@link NotificationOutcome#ACCEPTED}.
 *   <li>Receipt {@code DeviceNotRegistered} → {@link NotificationOutcome#REJECTED}.
 *   <li>Receipt other error → {@link NotificationOutcome#INVALID}.
 *   <li>Receipt absent after {@code maxReceiptAttempts} → {@link NotificationOutcome#UNKNOWN}.
 * </ul>
 *
 * <p>If Expo returns batch-level errors when fetching receipts, messages are left in the
 * queue and will become visible again after the visibility timeout.
 */
@Slf4j
public class PushReceiptQueueConsumer extends AbstractSqsConsumer {

    /**
     * Configuration parameters for {@link PushReceiptQueueConsumer}.
     * Groups the tuning knobs to keep the constructor within the 7-parameter limit.
     */
    public record Config(
        int maxReceiptAttempts,
        long drainTimeoutMs,
        long authFailureBackoffMs,
        String receiptQueueName
    ) {}

    private final ExpoGateway expoGateway;
    private final ExpoRateLimiter rateLimiter;
    private final PayloadEncryptor encryptor;
    private final ObjectMapper objectMapper;
    private final int maxReceiptAttempts;
    private final String receiptQueueName;
    private volatile String receiptQueueUrl;

    public PushReceiptQueueConsumer(
        SqsClient sqsClient,
        NotificationHandlerRegistry registry,
        ExpoGateway expoGateway,
        ExpoRateLimiter rateLimiter,
        PayloadEncryptor encryptor,
        ObjectMapper objectMapper,
        Config config
    ) {
        super(sqsClient, registry, "sqs-receipt-consumer", config.drainTimeoutMs(), config.authFailureBackoffMs());
        this.expoGateway = expoGateway;
        this.rateLimiter = rateLimiter;
        this.encryptor = encryptor;
        this.objectMapper = objectMapper;
        this.maxReceiptAttempts = config.maxReceiptAttempts();
        this.receiptQueueName = config.receiptQueueName();
    }

    @Override
    protected void onStart() {
        this.receiptQueueUrl = sqsClient.getQueueUrl(
            GetQueueUrlRequest.builder().queueName(receiptQueueName).build()
        ).queueUrl();
        log.info("Push Receipt Queue consumer configured — polling {}", receiptQueueUrl);
    }

    @Override
    protected void processOneBatch() throws InterruptedException {
        ReceiveMessageResponse response = sqsClient.receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(receiptQueueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                .build()
        );

        if (response == null || response.messages() == null) return;
        List<Message> sqsMessages = response.messages();
        if (sqsMessages.isEmpty()) return;

        log.debug("RQ received {} message(s)", sqsMessages.size());

        List<PushReceiptSqsMessage> receiptMessages = new ArrayList<>(sqsMessages.size());
        List<Message> validSqsMessages = new ArrayList<>(sqsMessages.size());

        for (Message m : sqsMessages) {
            PushReceiptSqsMessage parsed = parseAndValidate(m);
            if (parsed != null) {
                receiptMessages.add(parsed);
                validSqsMessages.add(m);
            }
        }

        if (receiptMessages.isEmpty()) return;

        List<String> ticketIds = receiptMessages.stream().map(PushReceiptSqsMessage::ticketId).toList();

        rateLimiter.acquire();

        try {
            PushReceiptResponse receiptResponse = expoGateway.getReceipts(ticketIds);
            dispatchReceipts(receiptMessages, validSqsMessages, receiptResponse);
        } catch (ExpoAuthException e) {
            // Without this, the loop would re-receive the same messages and hammer Expo
            // with doomed 401 calls. Messages stay in the queue; polling resumes after
            // the backoff — a fixed access token takes effect without a restart.
            criticalBackoff("Expo authentication failure on receipt fetch (" + e.getMessage()
                + ") — verify expo.push.access-token");
        } catch (Exception e) {
            log.error("Failed to fetch receipts for {} ticket(s) — will retry: {}", ticketIds.size(), e.getMessage());
            // Leave messages in queue; they become visible again after the visibility timeout.
        }
    }

    /**
     * Deserializes and validates one SQS message. Unparseable JSON, unknown (newer) schema
     * versions, and unregistered handlers are deploy-mismatch conditions a redeploy can fix:
     * they stay in the queue for redelivery/DLQ and are deleted only past the receive
     * ceiling. Returns null when the message did not join the batch.
     */
    private PushReceiptSqsMessage parseAndValidate(Message m) {
        PushReceiptSqsMessage parsed;
        try {
            parsed = objectMapper.readValue(m.body(), PushReceiptSqsMessage.class);
        } catch (Exception e) {
            resolveUnprocessable(receiptQueueUrl, m, maxReceiptAttempts,
                "RQ message failed to deserialise: " + e.getMessage());
            return null;
        }
        if (parsed.schemaVersion() > SqsNotificationMessage.CURRENT_SCHEMA_VERSION) {
            resolveUnprocessable(receiptQueueUrl, m, maxReceiptAttempts,
                "RQ message has schema version " + parsed.schemaVersion() + " but this node only "
                    + "understands <= " + SqsNotificationMessage.CURRENT_SCHEMA_VERSION
                    + " — is a newer producer deployed alongside an older consumer?");
            return null;
        }
        if (registry.getHandler(parsed.handlerId()) == null) {
            resolveUnprocessable(receiptQueueUrl, m, maxReceiptAttempts,
                "No NotificationResultHandler registered for handlerId=" + sanitize(parsed.handlerId())
                    + " (ticketId=" + sanitize(parsed.ticketId()) + ") — redeploy with the "
                    + "handler restored to recover this message");
            return null;
        }
        return parsed;
    }

    private void dispatchReceipts(
        List<PushReceiptSqsMessage> receiptMessages,
        List<Message> sqsMessages,
        PushReceiptResponse response
    ) {
        if (hasBatchErrors(response)) {
            String detail = response.getErrors().stream()
                .map(e -> e.getCode() + ": " + e.getMessage())
                .reduce((a, b) -> a + "; " + b).orElse("unknown");
            log.error("Expo batch-level errors on receipt fetch — will retry: {}", detail);
            return;
        }

        Map<String, PushReceipt> receiptMap = (response != null && response.getData() != null)
            ? response.getData() : Collections.emptyMap();

        List<Message> toDelete = new ArrayList<>(receiptMessages.size());
        for (int i = 0; i < receiptMessages.size(); i++) {
            if (processReceiptMessage(receiptMessages.get(i), sqsMessages.get(i), receiptMap)) {
                toDelete.add(sqsMessages.get(i));
            }
        }
        deleteMessageBatch(receiptQueueUrl, toDelete);
    }

    private static boolean hasBatchErrors(PushReceiptResponse response) {
        return response != null && response.getErrors() != null && !response.getErrors().isEmpty();
    }

    /**
     * Resolves one receipt message and returns {@code true} when it should be deleted —
     * the caller batches the deletes into a single {@code DeleteMessageBatch} call.
     */
    private boolean processReceiptMessage(
        PushReceiptSqsMessage rawMsg,
        Message sqsMsg,
        Map<String, PushReceipt> receiptMap
    ) {
        PushReceiptSqsMessage msg;
        try {
            msg = decrypt(rawMsg);
        } catch (Exception e) {
            // Undecryptable payload (rotated key, corrupt ciphertext) must fail terminally —
            // letting it escape would stall the consumer on endless redelivery. Expo already
            // accepted this notification, so the delivery state is UNKNOWN, not FAILED.
            log.error("Failed to decrypt RQ message — firing UNKNOWN: ticketId={} error={}",
                sanitize(rawMsg.ticketId()), e.getMessage());
            notifyHandler(result(NotificationOutcome.UNKNOWN,
                new PushReceiptSqsMessage(rawMsg.ticketId(), rawMsg.pushToken(),
                    rawMsg.correlationId(), Map.of(), rawMsg.handlerId(), null, null),
                rawMsg.ticketId(),
                "Payload decryption failed — verify the configured encryption key"));
            return true;
        }
        PushReceipt receipt = receiptMap.get(msg.ticketId());
        int receiveCount = parseReceiveCount(sqsMsg);

        if (receipt == null) {
            return handleMissingReceipt(msg, receiveCount);
        } else if (receipt.getStatus() == PushReceipt.StatusEnum.OK) {
            notifyHandler(result(NotificationOutcome.ACCEPTED, msg, msg.ticketId(), null));
            return true;
        } else {
            handleReceiptError(msg, receipt);
            return true;
        }
    }

    /** Returns {@code true} when the message is resolved and should be deleted. */
    private boolean handleMissingReceipt(PushReceiptSqsMessage msg, int receiveCount) {
        if (receiveCount >= maxReceiptAttempts) {
            log.warn("Receipt for ticketId={} not available after {} attempt(s) — notifying UNKNOWN",
                sanitize(msg.ticketId()), receiveCount);
            notifyHandler(result(NotificationOutcome.UNKNOWN, msg, msg.ticketId(), null));
            return true;
        }
        log.debug("Receipt for ticketId={} not yet available (attempt {}/{}) — will retry",
            sanitize(msg.ticketId()), receiveCount, maxReceiptAttempts);
        return false;
    }

    private void handleReceiptError(PushReceiptSqsMessage msg, PushReceipt receipt) {
        String errorDetail = ExpoErrors.errorOf(receipt);
        NotificationOutcome outcome = ExpoErrors.outcomeFor(errorDetail);
        if (outcome == NotificationOutcome.INVALID) {
            log.warn("Expo receipt error for ticketId={}: {}", sanitize(msg.ticketId()), errorDetail);
        }
        notifyHandler(result(outcome, msg, msg.ticketId(), errorDetail));
    }

    private PushReceiptSqsMessage decrypt(PushReceiptSqsMessage msg) {
        return new PushReceiptSqsMessage(
            msg.ticketId(),
            msg.pushToken(),
            msg.correlationId(),
            decryptMap(msg.metadata()),
            msg.handlerId(),
            encryptor.decrypt(msg.title()),
            encryptor.decrypt(msg.body())
        );
    }

    private Map<String, String> decryptMap(Map<String, String> map) {
        if (map == null) return Map.of();
        return map.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> encryptor.decrypt(e.getValue())
            ));
    }
}
