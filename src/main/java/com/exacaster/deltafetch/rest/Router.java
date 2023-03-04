package com.exacaster.deltafetch.rest;

import io.micronaut.http.HttpRequest;

import java.util.Collection;
import java.util.Optional;

public class Router {
    private final Collection<RequestHandler> handlers;


    public Router(Collection<RequestHandler> handlers) {
        this.handlers = handlers;
    }

    public Optional<ApiResponse<?>> route(HttpRequest<Void> request) {
        return handlers.stream()
                .flatMap(h -> h.handle(request).stream())
                .findFirst();
    }
}
