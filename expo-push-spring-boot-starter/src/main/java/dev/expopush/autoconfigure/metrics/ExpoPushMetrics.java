package dev.expopush.autoconfigure.metrics;

/**
 * Meter names and tags published by the starter when Micrometer is present.
 *
 * <ul>
 *   <li>{@value #SUBMISSIONS} (counter; tag {@code status}={@code accepted}|{@code rejected})
 *       — enqueue attempts. {@code rejected} means the command never entered the pipeline
 *       (validation failure, unknown handler, backend refusal).
 *   <li>{@value #RESULTS} (counter; tag {@code outcome}, lowercase
 *       {@code accepted|rejected|invalid|unknown|failed}) — terminal outcomes routed to
 *       handlers, across all backends.
 *   <li>{@value #API_CALLS} (timer; tags {@code operation}={@code send}|{@code get-receipts},
 *       {@code status}={@code ok}|{@code error}) — individual Expo HTTP attempts. Retries
 *       appear as additional attempts, so {@code api.calls[send].count} minus successful
 *       submissions approximates retry volume.
 *   <li>{@value #LOCAL_QUEUE_DEPTH} (gauge) — pending receipt checks in the local backend.
 *   <li>{@value #H2_PENDING} (gauge) — rows in the H2 backend's pending_receipts table.
 * </ul>
 */
public final class ExpoPushMetrics {

    public static final String SUBMISSIONS = "expo.push.submissions";
    public static final String RESULTS = "expo.push.results";
    public static final String API_CALLS = "expo.push.api.calls";
    public static final String LOCAL_QUEUE_DEPTH = "expo.push.local.receipt.queue.depth";
    public static final String H2_PENDING = "expo.push.h2.pending.receipts";

    private ExpoPushMetrics() {}
}
