package dev.expopush.core;

import dev.expopush.core.api.PushApi;
import dev.expopush.core.api.model.PushReceiptRequest;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.api.model.PushTicketResponse;
import dev.expopush.core.api.model.PushMessage;
import io.github.resilience4j.retry.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Thin HTTP gateway to the Expo Push Notification API.
 *
 * <p>All Expo API types are confined to this class and the SQS consumers. Callers
 * interact only with neutral types from {@code expo-push-api}.
 *
 * <p>This class has no threading, queuing, or callback logic. Batching, rate-limiting,
 * and result routing are the responsibility of the backend implementation.
 */
@Slf4j
@RequiredArgsConstructor
public class ExpoGateway {

    private final PushApi pushApi;
    private final Retry sendRetry;
    private final Retry receiptRetry;

    /**
     * Sends a batch of push notifications to Expo and returns the raw ticket response.
     * Transient errors (rate-limit, 5xx) are retried with exponential backoff.
     * Non-retryable errors (401, 413) are thrown immediately.
     *
     * @param messages up to 100 Expo push messages (Expo hard limit per request)
     */
    public PushTicketResponse sendNotifications(List<PushMessage> messages) {
        log.debug("Sending batch of {} push notification(s) to Expo", messages.size());
        return sendRetry.executeSupplier(() -> pushApi.sendNotifications(messages)).getBody();
    }

    /**
     * Fetches delivery receipts for the given ticket IDs from Expo.
     *
     * @param ticketIds ticket IDs returned by a previous {@link #sendNotifications} call
     */
    public PushReceiptResponse getReceipts(List<String> ticketIds) {
        log.debug("Fetching receipts for {} ticket(s)", ticketIds.size());
        PushReceiptRequest request = new PushReceiptRequest();
        request.setIds(ticketIds);
        return receiptRetry.executeSupplier(() -> pushApi.getReceipts(request)).getBody();
    }
}
