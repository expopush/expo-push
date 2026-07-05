package dev.expopush.autoconfigure.metrics;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.autoconfigure.ExpoH2BackendAutoConfiguration;
import dev.expopush.autoconfigure.ExpoLocalBackendAutoConfiguration;
import dev.expopush.autoconfigure.ExpoPushAutoConfiguration;
import dev.expopush.autoconfigure.ExpoPushServiceAutoConfiguration;
import dev.expopush.core.api.PushApi;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Metrics for the Expo push pipeline — active only when micrometer-core is on the
 * classpath (it is an optional dependency of this starter) and the application exposes a
 * {@link MeterRegistry} bean. Publishes the meters documented on {@link ExpoPushMetrics}.
 *
 * <p>Instrumentation decorates existing seams rather than touching backend code:
 * the {@link AsyncNotificationService} (submissions), the
 * {@link NotificationHandlerRegistry} (terminal outcomes for every backend), and the
 * {@link PushApi} Feign client (per-attempt Expo call timings).
 */
@AutoConfiguration(after = {
    ExpoPushAutoConfiguration.class,
    ExpoLocalBackendAutoConfiguration.class,
    ExpoH2BackendAutoConfiguration.class,
    ExpoPushServiceAutoConfiguration.class
})
@ConditionalOnClass(MeterRegistry.class)
public class ExpoPushMetricsAutoConfiguration {

    /**
     * Static so the post-processor registers before regular bean instantiation. The
     * {@link MeterRegistry} is resolved lazily on first wrap; when the application has
     * no registry bean, every bean passes through untouched.
     */
    @Bean
    public static BeanPostProcessor expoPushMeteringPostProcessor(
        ObjectProvider<MeterRegistry> meterRegistryProvider
    ) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                // Type-check BEFORE touching the provider: resolving the MeterRegistry for
                // arbitrary beans would trigger its creation while its own @Configuration
                // class is mid-post-processing — a circular reference.
                boolean isTarget = (bean instanceof AsyncNotificationService
                        || bean instanceof NotificationHandlerRegistry
                        || bean instanceof PushApi)
                    && !(bean instanceof MeteredAsyncNotificationService
                        || bean instanceof MeteredNotificationHandlerRegistry
                        || bean instanceof MeteredPushApi);
                if (!isTarget) {
                    return bean;
                }
                MeterRegistry registry = meterRegistryProvider.getIfAvailable();
                if (registry == null) {
                    return bean;
                }
                if (bean instanceof AsyncNotificationService service) {
                    return new MeteredAsyncNotificationService(service, registry);
                }
                if (bean instanceof NotificationHandlerRegistry handlerRegistry) {
                    return new MeteredNotificationHandlerRegistry(handlerRegistry, registry);
                }
                return new MeteredPushApi((PushApi) bean, registry);
            }
        };
    }

    /** Gauge for the local backend's in-memory receipt queue. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "dev.expopush.backend.local.LocalReceiptOrchestrator")
    @ConditionalOnBean(type = "dev.expopush.backend.local.LocalReceiptOrchestrator")
    static class LocalQueueDepthMetricsConfiguration {

        @Bean
        MeterBinder expoLocalQueueDepthGauge(dev.expopush.backend.local.LocalReceiptOrchestrator orchestrator) {
            return registry -> Gauge.builder(ExpoPushMetrics.LOCAL_QUEUE_DEPTH, orchestrator,
                    o -> o.queueDepth())
                .description("Receipt checks queued in the local backend")
                .register(registry);
        }
    }

    /** Gauge over the H2 backend's pending_receipts table (one count query per scrape). */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "dev.expopush.backend.h2.H2ReceiptOrchestrator")
    @ConditionalOnBean(name = "expoH2JdbcTemplate")
    static class H2PendingMetricsConfiguration {

        @Bean
        MeterBinder expoH2PendingGauge(JdbcTemplate expoH2JdbcTemplate) {
            return registry -> Gauge.builder(ExpoPushMetrics.H2_PENDING, expoH2JdbcTemplate,
                    jdbc -> {
                        Integer count = jdbc.queryForObject(
                            "SELECT COUNT(*) FROM pending_receipts", Integer.class);
                        return count == null ? 0 : count;
                    })
                .description("Rows in the H2 backend's pending_receipts table")
                .register(registry);
        }
    }
}
