package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * Verifies the {@link org.springframework.context.SmartLifecycle} contract for
 * {@link AbstractSqsConsumer}: start/stop semantics, idempotent start, and async drain.
 */
class AbstractSqsConsumerLifecycleTest {

    @Test
    void isRunningFalseBeforeStart() {
        assertThat(new ImmediateReturnConsumer().isRunning()).isFalse();
    }

    @Test
    void isRunningTrueAfterStart() {
        AbstractSqsConsumer consumer = new ImmediateReturnConsumer();
        consumer.start();
        try {
            assertThat(consumer.isRunning()).isTrue();
        } finally {
            consumer.stop();
        }
    }

    @Test
    @Timeout(5)
    void startBeginsPolling() throws InterruptedException {
        CountDownLatch polled = new CountDownLatch(1);
        AbstractSqsConsumer consumer = new LatchOnFirstPollConsumer(polled);
        consumer.start();
        try {
            assertThat(polled.await(3, TimeUnit.SECONDS)).isTrue();
        } finally {
            consumer.stop();
        }
    }

    @Test
    @Timeout(5)
    void stopAsyncCallbackFiresAfterDrain() throws InterruptedException {
        CountDownLatch callbackFired = new CountDownLatch(1);
        AbstractSqsConsumer consumer = new ImmediateReturnConsumer();
        consumer.start();

        consumer.stop(callbackFired::countDown);

        assertThat(callbackFired.await(4, TimeUnit.SECONDS))
            .as("stop(Runnable) callback must fire after consumer thread exits")
            .isTrue();
        assertThat(consumer.isRunning()).isFalse();
    }

    @Test
    @Timeout(5)
    void stopTerminatesPolling() {
        AtomicInteger pollCount = new AtomicInteger();
        AbstractSqsConsumer consumer = new CountingConsumer(pollCount);
        consumer.start();
        await().atMost(500, TimeUnit.MILLISECONDS).until(() -> pollCount.get() > 0);
        consumer.stop();
        int countAtStop = pollCount.get();
        await().during(200, TimeUnit.MILLISECONDS).atMost(400, TimeUnit.MILLISECONDS)
            .until(() -> pollCount.get() == countAtStop);
    }

    @Test
    void secondStartIsIdempotent() {
        AbstractSqsConsumer consumer = new ImmediateReturnConsumer();
        consumer.start();
        try {
            consumer.start(); // must not spawn a second thread
            assertThat(consumer.isRunning()).isTrue();
        } finally {
            consumer.stop();
        }
    }

    // ─── Test consumer implementations ───────────────────────────────────────

    static class ImmediateReturnConsumer extends AbstractSqsConsumer {
        ImmediateReturnConsumer() {
            super(mock(SqsClient.class), mock(NotificationHandlerRegistry.class), "test-immediate", 30000);
        }
        @Override protected void processOneBatch() throws InterruptedException { Thread.yield(); }
    }

    static class LatchOnFirstPollConsumer extends AbstractSqsConsumer {
        private final CountDownLatch latch;
        LatchOnFirstPollConsumer(CountDownLatch latch) {
            super(mock(SqsClient.class), mock(NotificationHandlerRegistry.class), "test-latch", 30000);
            this.latch = latch;
        }
        @Override protected void processOneBatch() throws InterruptedException {
            latch.countDown();
            Thread.yield();
        }
    }

    static class CountingConsumer extends AbstractSqsConsumer {
        private final AtomicInteger counter;
        CountingConsumer(AtomicInteger counter) {
            super(mock(SqsClient.class), mock(NotificationHandlerRegistry.class), "test-counting", 30000);
            this.counter = counter;
        }
        @Override protected void processOneBatch() throws InterruptedException {
            counter.incrementAndGet();
            Thread.yield();
        }
    }
}
