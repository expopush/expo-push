package dev.expopush.backend.api;

import dev.expopush.api.LogMasker;
import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes terminal {@link NotificationResult}s to the registered handler. The single
 * implementation of the "look up handler, invoke, swallow-and-log handler exceptions"
 * contract that every backend previously copied — handler failures must never disrupt
 * the delivery pipeline, and the routing behavior must not drift between backends.
 */
public final class ResultDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ResultDispatcher.class);

    private final NotificationHandlerRegistry registry;

    public ResultDispatcher(NotificationHandlerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Delivers the result to its handler. Handler exceptions are caught and logged —
     * the callback is a best-effort terminal sink. A missing handler is logged by the
     * registry itself.
     */
    public void dispatch(NotificationResult result) {
        NotificationResultHandler handler = registry.getHandler(result.handlerId());
        if (handler == null) {
            return;
        }
        try {
            handler.handleResult(result);
        } catch (Exception e) {
            log.error("Handler failure requires manual intervention: handlerId={} outcome={} "
                    + "correlationId={} ticketId={}",
                LogMasker.sanitize(result.handlerId()), result.outcome(),
                LogMasker.sanitize(result.correlationId()), LogMasker.sanitize(result.ticketId()), e);
        }
    }

    /** Builds a result echoing the command's identity fields. */
    public static NotificationResult result(
        NotificationOutcome outcome, NotificationCommand cmd, String ticketId, String errorDetail
    ) {
        return new NotificationResult(
            outcome, cmd.handlerId(), cmd.correlationId(), cmd.pushToken(),
            cmd.title(), cmd.body(), ticketId, errorDetail, cmd.metadata());
    }
}
