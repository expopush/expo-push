package dev.expopush.autoconfigure.metrics;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResultHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.EnumMap;
import java.util.Map;

/**
 * Decorates the {@link NotificationHandlerRegistry} so every handler it returns counts the
 * terminal outcome before delegating. Because all backends route results through the
 * registry, this one seam meters outcomes for SQS, H2, and local alike — no backend code
 * is involved.
 */
public final class MeteredNotificationHandlerRegistry implements NotificationHandlerRegistry {

    private final NotificationHandlerRegistry delegate;
    private final Map<NotificationOutcome, Counter> outcomeCounters;

    public MeteredNotificationHandlerRegistry(NotificationHandlerRegistry delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.outcomeCounters = new EnumMap<>(NotificationOutcome.class);
        for (NotificationOutcome outcome : NotificationOutcome.values()) {
            outcomeCounters.put(outcome, Counter.builder(ExpoPushMetrics.RESULTS)
                .tag("outcome", outcome.name().toLowerCase())
                .register(registry));
        }
    }

    @Override
    public NotificationResultHandler getHandler(String handlerId) {
        NotificationResultHandler handler = delegate.getHandler(handlerId);
        if (handler == null) {
            return null;
        }
        return new NotificationResultHandler() {
            @Override
            public String handlerId() {
                return handler.handlerId();
            }

            @Override
            public void handleResult(dev.expopush.api.NotificationResult result) {
                // Count before delegating: the outcome is a fact even if the handler throws
                // (handler exceptions are caught and logged by the caller).
                Counter counter = outcomeCounters.get(result.outcome());
                if (counter != null) {
                    counter.increment();
                }
                handler.handleResult(result);
            }
        };
    }
}
