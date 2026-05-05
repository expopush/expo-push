package dev.expopush.core;

import dev.expopush.core.exception.ExpoAuthException;
import dev.expopush.core.exception.ExpoPayloadTooLargeException;
import dev.expopush.core.exception.ExpoRateLimitException;
import dev.expopush.core.exception.ExpoServerException;
import dev.expopush.core.ratelimit.LocalExpoRateLimiter;
import feign.RequestTemplate;
import feign.Response;
import io.github.resilience4j.ratelimiter.RateLimiter;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ExpoCoreUnitTests {

    @Test
    void bearerTokenInterceptorAddsHeader() {
        ExpoBearerTokenInterceptor interceptor = new ExpoBearerTokenInterceptor("secret");
        RequestTemplate template = new RequestTemplate();
        
        interceptor.apply(template);
        
        assertThat(template.headers().get("Authorization")).containsExactly("Bearer secret");
    }

    @Test
    void clientErrorDecoderMapsStatuses() {
        ExpoClientErrorDecoder decoder = new ExpoClientErrorDecoder();
        
        assertThat(decoder.decode("test", mockResponse(401)))
            .isInstanceOf(ExpoAuthException.class);
        assertThat(decoder.decode("test", mockResponse(413)))
            .isInstanceOf(ExpoPayloadTooLargeException.class);
        assertThat(decoder.decode("test", mockResponse(429)))
            .isInstanceOf(ExpoRateLimitException.class);
        assertThat(decoder.decode("test", mockResponse(500)))
            .isInstanceOf(ExpoServerException.class);
    }

    @Test
    void localRateLimiterAcquires() {
        RateLimiter mockRl = mock(RateLimiter.class);
        LocalExpoRateLimiter rateLimiter = new LocalExpoRateLimiter(mockRl);
        
        rateLimiter.acquire();
        
        verify(mockRl).acquirePermission();
    }

    private Response mockResponse(int status) {
        return Response.builder()
            .status(status)
            .reason("Error")
            .request(feign.Request.create(feign.Request.HttpMethod.GET, "/test", 
                Collections.emptyMap(), feign.Request.Body.empty(), null))
            .build();
    }
}
