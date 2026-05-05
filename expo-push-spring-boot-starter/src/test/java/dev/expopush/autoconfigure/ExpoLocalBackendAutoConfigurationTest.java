package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.local.LocalNotificationBackend;
import dev.expopush.backend.local.LocalReceiptOrchestrator;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.PushApi;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ExpoLocalBackendAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(
            ExpoPushAutoConfiguration.class,
            ExpoLocalBackendAutoConfiguration.class,
            ExpoPushServiceAutoConfiguration.class
        ))
        .withUserConfiguration(MockPushApiConfig.class, MockRegistryConfig.class);

    // ─── Activation guard ─────────────────────────────────────────────────────

    @Test
    void localBeansAbsentWithoutBackendProperty() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).doesNotHaveBean(LocalNotificationBackend.class);
                assertThat(ctx).doesNotHaveBean(LocalReceiptOrchestrator.class);
            });
    }

    @Test
    void localBeansAbsentWhenBackendIsNotLocal() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).doesNotHaveBean(LocalNotificationBackend.class);
                assertThat(ctx).doesNotHaveBean(LocalReceiptOrchestrator.class);
            });
    }

    // ─── Activation ───────────────────────────────────────────────────────────

    @Test
    void localReceiptOrchestratorCreatedWhenBackendIsLocal() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=local",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).hasSingleBean(LocalReceiptOrchestrator.class));
    }

    @Test
    void notificationBackendIsLocalNotificationBackendWhenBackendIsLocal() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=local",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(NotificationBackend.class);
                assertThat(ctx.getBean(NotificationBackend.class))
                    .isInstanceOf(LocalNotificationBackend.class);
            });
    }

    @Test
    void localPropertiesApplied() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=local",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.local.max-queue-size=500",
                "expo.push.local.max-receipt-attempts=5",
                "expo.push.local.receipt-delay-seconds=120"
            )
            .run(ctx -> {
                ExpoPushProperties props = ctx.getBean(ExpoPushProperties.class);
                assertThat(props.getLocal().getMaxQueueSize()).isEqualTo(500);
                assertThat(props.getLocal().getMaxReceiptAttempts()).isEqualTo(5);
                assertThat(props.getLocal().getReceiptDelaySeconds()).isEqualTo(120);
            });
    }

    // ─── Bean override ────────────────────────────────────────────────────────

    @Test
    void customLocalReceiptOrchestratorOverridesDefault() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=local",
                "expo.push.security.encrypt-payloads=false"
            )
            .withUserConfiguration(CustomOrchestratorConfig.class)
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(LocalReceiptOrchestrator.class);
                assertThat(ctx.getBean(LocalReceiptOrchestrator.class))
                    .isSameAs(ctx.getBean("customOrchestrator"));
            });
    }

    // ─── Test support ─────────────────────────────────────────────────────────

    @Configuration
    static class MockPushApiConfig {
        @Bean
        PushApi mockPushApi() { return mock(PushApi.class); }
    }

    @Configuration
    static class MockRegistryConfig {
        @Bean
        NotificationHandlerRegistry mockRegistry() { return mock(NotificationHandlerRegistry.class); }
    }

    @Configuration
    static class CustomOrchestratorConfig {
        @Bean("customOrchestrator")
        LocalReceiptOrchestrator customOrchestrator(ExpoGateway gateway, NotificationHandlerRegistry registry) {
            return new LocalReceiptOrchestrator(gateway, registry, 3, 1000L, 100);
        }
    }
}
