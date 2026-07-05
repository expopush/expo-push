package dev.expopush.autoconfigure.metrics;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationCommand;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Decorates the {@link AsyncNotificationService} with a submissions counter.
 * {@code status=accepted} means the command entered the pipeline; {@code status=rejected}
 * means it did not (validation failure, unknown handler, backend refusal) and no callback
 * will ever fire for it.
 */
public final class MeteredAsyncNotificationService implements AsyncNotificationService {

    private final AsyncNotificationService delegate;
    private final Counter accepted;
    private final Counter rejected;

    public MeteredAsyncNotificationService(AsyncNotificationService delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.accepted = Counter.builder(ExpoPushMetrics.SUBMISSIONS)
            .tag("status", "accepted").register(registry);
        this.rejected = Counter.builder(ExpoPushMetrics.SUBMISSIONS)
            .tag("status", "rejected").register(registry);
    }

    @Override
    public void enqueue(NotificationCommand command) {
        try {
            delegate.enqueue(command);
            accepted.increment();
        } catch (RuntimeException e) {
            rejected.increment();
            throw e;
        }
    }
}
