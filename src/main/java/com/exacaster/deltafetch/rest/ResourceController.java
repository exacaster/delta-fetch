package com.exacaster.deltafetch.rest;

import com.exacaster.deltafetch.search.SearchResult;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.List;
import java.util.Optional;

@Controller("/")
public class ResourceController {
    private final List<ResourceRequestHandler> handlers;

    public ResourceController(List<ResourceRequestHandler> handlers) {
        this.handlers = handlers;
    }

    @Get(value = "/{+path*}", consumes = MediaType.ALL)
    public Optional<SearchResult> index(HttpRequest request) {
        return handlers.stream()
                .flatMap(h -> h.handle(request).stream())
                .findFirst();
    }
}
