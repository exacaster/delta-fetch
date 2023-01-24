package com.exacaster.deltafetch.rest;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

import java.util.Optional;

@Controller("/")
public class APIController {
    private final Router router;

    public APIController(Router router) {
        this.router = router;
    }

    @Get(value = "/{+path*}", consumes = MediaType.ALL)
    public Optional<APIResponse> index(HttpRequest request) {
        return router.route(request);
    }
}
