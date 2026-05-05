package dev.expopush.core;

import feign.RequestInterceptor;
import feign.RequestTemplate;

public class ExpoBearerTokenInterceptor implements RequestInterceptor {

    private final String accessToken;

    public ExpoBearerTokenInterceptor(String accessToken) {
        this.accessToken = accessToken;
    }

    @Override
    public void apply(RequestTemplate template) {
        template.header("Authorization", "Bearer " + accessToken);
    }
}
