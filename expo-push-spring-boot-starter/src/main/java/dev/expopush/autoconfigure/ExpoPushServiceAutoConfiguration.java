package dev.expopush.autoconfigure;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.backend.api.NotificationBackend;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;

/**
 * Wires the public {@link AsyncNotificationService} to the selected backend.
 *
 * <p>Runs after both core and backend auto-configurations so that
 * {@link ConditionalOnSingleCandidate} can see any registered {@link NotificationBackend}
 * implementations.
 */
@AutoConfiguration(after = {ExpoPushAutoConfiguration.class, ExpoSqsBackendAutoConfiguration.class})
public class ExpoPushServiceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(NotificationBackend.class)
    public AsyncNotificationService asyncNotificationService(NotificationBackend backend) {
        return new DefaultAsyncNotificationService(backend);
    }
}
