package dev.expopush.autoconfigure.metrics;

import dev.expopush.core.api.PushApi;
import dev.expopush.core.api.model.PushMessage;
import dev.expopush.core.api.model.PushReceiptRequest;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.api.model.PushTicketResponse;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * Times every Expo HTTP attempt. Because the Resilience4j retry wraps the {@link PushApi}
 * call, each retry attempt is recorded individually — the count of {@code send} timings
 * relative to submissions approximates retry volume.
 */
public final class MeteredPushApi implements PushApi {

    private final PushApi delegate;
    private final MeterRegistry registry;

    public MeteredPushApi(PushApi delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
    }

    @Override
    public ResponseEntity<PushTicketResponse> sendNotifications(List<PushMessage> pushMessage) {
        return timed("send", () -> delegate.sendNotifications(pushMessage));
    }

    @Override
    public ResponseEntity<PushReceiptResponse> getReceipts(PushReceiptRequest pushReceiptRequest) {
        return timed("get-receipts", () -> delegate.getReceipts(pushReceiptRequest));
    }

    private <T> T timed(String operation, java.util.function.Supplier<T> call) {
        Timer.Sample sample = Timer.start(registry);
        try {
            T result = call.get();
            sample.stop(Timer.builder(ExpoPushMetrics.API_CALLS)
                .tag("operation", operation).tag("status", "ok").register(registry));
            return result;
        } catch (RuntimeException e) {
            sample.stop(Timer.builder(ExpoPushMetrics.API_CALLS)
                .tag("operation", operation).tag("status", "error").register(registry));
            throw e;
        }
    }
}
