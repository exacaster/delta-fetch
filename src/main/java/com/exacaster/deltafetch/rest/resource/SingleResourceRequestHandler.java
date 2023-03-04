package com.exacaster.deltafetch.rest.resource;

import com.exacaster.deltafetch.configuration.ResourceConfiguration.Resource;
import com.exacaster.deltafetch.rest.ApiResponse;
import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import java.util.Map;
import java.util.Optional;

public class SingleResourceRequestHandler implements RequestHandler<Map<String, Object>> {

    private final RequestLookup lookup;

    public SingleResourceRequestHandler(SearchService searchService, Resource resource) {
        this.lookup = new RequestLookup(searchService, resource);
    }

    @Override
    public Optional<ApiResponse<Map<String, Object>>> handle(HttpRequest<Void> request) {
        return lookup.handleStream(request, 1)
                .findFirst()
                .map(pair -> new ApiResponse<>(pair.getKey(), pair.getValue()));
    }
}
