package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.local.LocalNotificationBackend;
import dev.expopush.backend.local.LocalReceiptOrchestrator;
import dev.expopush.core.ExpoGateway;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Auto-configuration for the local-only DelayQueue-backed notification backend.
 */
@AutoConfiguration(after = ExpoPushAutoConfiguration.class)
@ConditionalOnClass(LocalNotificationBackend.class)
@ConditionalOnProperty(prefix = "expo.push", name = "backend", havingValue = "local")
@EnableConfigurationProperties(ExpoPushProperties.class)
public class ExpoLocalBackendAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public LocalReceiptOrchestrator localReceiptOrchestrator(
            ExpoGateway expoGateway,
            NotificationHandlerRegistry registry,
            ExpoPushProperties properties) {
        ExpoPushProperties.Local local = properties.getLocal();
        return new LocalReceiptOrchestrator(
            expoGateway,
            registry,
            local.getMaxReceiptAttempts(),
            TimeUnit.SECONDS.toMillis(local.getReceiptDelaySeconds()),
            local.getMaxQueueSize()
        );
    }

    /**
     * Single-threaded so sends stay sequential (matching the throughput characteristics of
     * the previous synchronous behaviour) while keeping the Expo call — and its retry/backoff
     * cycle — off the caller's thread.
     */
    @Bean(name = "expoLocalSubmissionExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "expoLocalSubmissionExecutor")
    public ExecutorService expoLocalSubmissionExecutor() {
        return Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("expo-local-submitter").factory());
    }

    @Bean
    @ConditionalOnMissingBean
    public NotificationBackend localNotificationBackend(
            ExpoGateway expoGateway,
            LocalReceiptOrchestrator orchestrator,
            NotificationHandlerRegistry registry,
            ExecutorService expoLocalSubmissionExecutor,
            ExpoPushProperties properties) {
        return new LocalNotificationBackend(
            expoGateway,
            orchestrator,
            registry,
            expoLocalSubmissionExecutor,
            TimeUnit.SECONDS.toMillis(properties.getLocal().getReceiptDelaySeconds())
        );
    }
}
