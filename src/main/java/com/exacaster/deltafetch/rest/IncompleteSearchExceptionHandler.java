package com.exacaster.deltafetch.rest;

import com.exacaster.deltafetch.search.IncompleteSearchException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import javax.inject.Singleton;

@Produces
@Singleton
public class IncompleteSearchExceptionHandler
        implements ExceptionHandler<IncompleteSearchException, HttpResponse<JsonError>> {

    @Override
    public HttpResponse<JsonError> handle(HttpRequest request, IncompleteSearchException exception) {
        return HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new JsonError(exception.getMessage()));
    }
}
