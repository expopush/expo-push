package dev.expopush.api;

/**
 * Public façade for submitting push notifications into the async delivery pipeline.
 *
 * <p>The implementation is provided by the starter's auto-configuration. Callers
 * submit a {@link NotificationCommand} and eventually receive a {@link NotificationResult}
 * via their registered {@link NotificationResultHandler}. The internal mechanics of
 * batching, transport, and receipt polling are not visible to callers.
 *
 * <p>Throws {@link NotificationSubmissionException} (unchecked) if the command cannot
 * be accepted into the pipeline. In that case no callback will fire for the command.
 */
public interface AsyncNotificationService {

    /**
     * Accepts a notification command for asynchronous delivery.
     *
     * @throws NotificationSubmissionException if the command cannot be submitted
     */
    void enqueue(NotificationCommand command);
}
