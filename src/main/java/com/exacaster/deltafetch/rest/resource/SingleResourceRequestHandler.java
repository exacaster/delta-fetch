package com.exacaster.deltafetch.rest.resource;

import com.exacaster.deltafetch.configuration.ResourceConfiguration.Resource;
import com.exacaster.deltafetch.rest.ApiResponse;
import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import java.util.Map;
import java.util.Optional;

public class SingleResourceRequestHandler extends ResourceRequestHandler implements
        RequestHandler<Map<String, Object>> {

    public SingleResourceRequestHandler(SearchService searchService, Resource resource) {
        super(searchService, resource);
    }

    @Override
    public Optional<ApiResponse<Map<String, Object>>> handle(HttpRequest request) {
        return handleStream(request, 1)
                .findFirst()
                .map(pair -> new ApiResponse<>(pair.getKey(), pair.getValue()));
    }
}
