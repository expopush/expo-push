package dev.expopush.autoconfigure.metrics;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.autoconfigure.ExpoPushAutoConfiguration;
import dev.expopush.autoconfigure.ExpoPushServiceAutoConfiguration;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.core.api.PushApi;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ExpoPushMetricsAutoConfigurationTest {

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                ExpoPushAutoConfiguration.class,
                ExpoPushServiceAutoConfiguration.class,
                ExpoPushMetricsAutoConfiguration.class
            ))
            .withUserConfiguration(TestBeansConfig.class)
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            );
    }

    // ─── Activation ───────────────────────────────────────────────────────────

    @Test
    void serviceAndRegistryAreDecoratedWhenMeterRegistryPresent() {
        runner()
            .withUserConfiguration(MeterRegistryConfig.class)
            .run(ctx -> {
                assertThat(ctx.getBean(AsyncNotificationService.class))
                    .isInstanceOf(MeteredAsyncNotificationService.class);
                assertThat(ctx.getBean(NotificationHandlerRegistry.class))
                    .isInstanceOf(MeteredNotificationHandlerRegistry.class);
                assertThat(ctx.getBean(PushApi.class))
                    .isInstanceOf(MeteredPushApi.class);
            });
    }

    @Test
    void beansAreUntouchedWithoutMeterRegistryBean() {
        runner()
            .run(ctx -> {
                assertThat(ctx.getBean(AsyncNotificationService.class))
                    .isNotInstanceOf(MeteredAsyncNotificationService.class);
                assertThat(ctx.getBean(NotificationHandlerRegistry.class))
                    .isNotInstanceOf(MeteredNotificationHandlerRegistry.class);
            });
    }

    @Test
    void beansAreUntouchedWhenMicrometerNotOnClasspath() {
        runner()
            .withClassLoader(new FilteredClassLoader(MeterRegistry.class))
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(AsyncNotificationService.class);
                assertThat(ctx.getBean(AsyncNotificationService.class).getClass().getName())
                    .doesNotContain("Metered");
            });
    }

    // ─── Meters ───────────────────────────────────────────────────────────────

    @Test
    void submissionsCounterCountsAcceptedAndRejected() {
        runner()
            .withUserConfiguration(MeterRegistryConfig.class)
            .run(ctx -> {
                MeterRegistry meters = ctx.getBean(MeterRegistry.class);
                AsyncNotificationService service = ctx.getBean(AsyncNotificationService.class);

                service.enqueue(new NotificationCommand(
                    "tok", "t", "b", "corr-1", Map.of(), "test-handler"));
                assertThatThrownBy(() -> service.enqueue(new NotificationCommand(
                    "tok", "t", "b", "corr-2", Map.of(), "nobody-home")))
                    .isInstanceOf(NotificationSubmissionException.class);

                assertThat(meters.counter(ExpoPushMetrics.SUBMISSIONS, "status", "accepted").count())
                    .isEqualTo(1.0);
                assertThat(meters.counter(ExpoPushMetrics.SUBMISSIONS, "status", "rejected").count())
                    .isEqualTo(1.0);
            });
    }

    @Test
    void resultsCounterTagsOutcome() {
        runner()
            .withUserConfiguration(MeterRegistryConfig.class)
            .run(ctx -> {
                MeterRegistry meters = ctx.getBean(MeterRegistry.class);
                NotificationHandlerRegistry registry = ctx.getBean(NotificationHandlerRegistry.class);

                NotificationResultHandler handler = registry.getHandler("test-handler");
                handler.handleResult(result(NotificationOutcome.ACCEPTED));
                handler.handleResult(result(NotificationOutcome.ACCEPTED));
                handler.handleResult(result(NotificationOutcome.REJECTED));

                assertThat(meters.counter(ExpoPushMetrics.RESULTS, "outcome", "accepted").count())
                    .isEqualTo(2.0);
                assertThat(meters.counter(ExpoPushMetrics.RESULTS, "outcome", "rejected").count())
                    .isEqualTo(1.0);
            });
    }

    private static NotificationResult result(NotificationOutcome outcome) {
        return new NotificationResult(outcome, "test-handler", "corr-1",
            "tok", "t", "b", "ticket-1", null, Map.of());
    }

    // ─── Test support ─────────────────────────────────────────────────────────

    @Configuration(proxyBeanMethods = false)
    static class MeterRegistryConfig {
        @Bean
        MeterRegistry simpleMeterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class TestBeansConfig {
        @Bean
        PushApi mockPushApi() {
            return mock(PushApi.class);
        }

        @Bean
        NotificationBackend mockBackend() {
            return mock(NotificationBackend.class);
        }

        @Bean
        NotificationResultHandler testHandler() {
            return new NotificationResultHandler() {
                @Override
                public String handlerId() {
                    return "test-handler";
                }

                @Override
                public void handleResult(NotificationResult result) {
                    // no-op sink for metering tests
                }
            };
        }
    }
}
