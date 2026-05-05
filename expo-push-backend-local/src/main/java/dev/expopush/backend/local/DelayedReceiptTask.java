package dev.expopush.backend.local;

import dev.expopush.api.NotificationCommand;
import lombok.Getter;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A task representing a pending delivery receipt check.
 */
@Getter
public class DelayedReceiptTask implements Delayed {

    private final String ticketId;
    private final NotificationCommand command;
    private final long executeAtNanos;
    private final int attempt;

    public DelayedReceiptTask(String ticketId, NotificationCommand command, long delayMillis, int attempt) {
        this.ticketId = ticketId;
        this.command = command;
        this.executeAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis);
        this.attempt = attempt;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(executeAtNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (this == o) return 0;
        return Long.compare(executeAtNanos, ((DelayedReceiptTask) o).executeAtNanos);
    }
}
