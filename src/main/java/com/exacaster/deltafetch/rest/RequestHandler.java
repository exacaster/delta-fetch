package com.exacaster.deltafetch.rest;

import io.micronaut.http.HttpRequest;

import java.util.Optional;

public interface RequestHandler<T> {
    Optional<APIResponse<T>> handle(HttpRequest request);
}
