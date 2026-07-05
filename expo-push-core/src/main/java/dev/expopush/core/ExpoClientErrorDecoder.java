package dev.expopush.core;

import feign.Response;
import feign.codec.ErrorDecoder;
import dev.expopush.core.exception.ExpoAuthException;
import dev.expopush.core.exception.ExpoPayloadTooLargeException;
import dev.expopush.core.exception.ExpoRateLimitException;
import dev.expopush.core.exception.ExpoServerException;

public class ExpoClientErrorDecoder implements ErrorDecoder {

    private final ErrorDecoder defaultDecoder = new Default();

    @Override
    public Exception decode(String methodKey, Response response) {
        return switch (response.status()) {
            case 401 -> new ExpoAuthException(
                "Expo authentication failed — verify the configured access token.");
            case 413 -> new ExpoPayloadTooLargeException(
                "Expo rejected the batch as too large (>100 messages or >4 KB total payload).");
            case 429 -> new ExpoRateLimitException(
                "Expo rate limit exceeded — will retry honoring Retry-After when present.",
                retryAfterSeconds(response));
            default -> response.status() >= 500
                ? new ExpoServerException("Expo server error %d — will retry with exponential backoff."
                    .formatted(response.status()))
                : defaultDecoder.decode(methodKey, response);
        };
    }

    /** Parses a delta-seconds {@code Retry-After} header; null when absent or non-numeric. */
    private static Long retryAfterSeconds(Response response) {
        var values = response.headers().get("Retry-After");
        if (values == null || values.isEmpty()) {
            values = response.headers().get("retry-after");
        }
        if (values == null || values.isEmpty()) {
            return null;
        }
        try {
            long seconds = Long.parseLong(values.iterator().next().trim());
            return seconds >= 0 ? seconds : null;
        } catch (NumberFormatException e) {
            return null; // HTTP-date form or garbage — fall back to exponential backoff
        }
    }
}
