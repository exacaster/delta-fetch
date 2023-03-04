package com.exacaster.deltafetch.rest;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Optional;

@Controller("/")
public class ApiController {
    private final Router router;

    public ApiController(Router router) {
        this.router = router;
    }

    @Get(value = "/{+path*}", consumes = MediaType.ALL)
    public Optional<ApiResponse<?>> index(HttpRequest<Void> request) {
        return router.route(request);
    }
}
