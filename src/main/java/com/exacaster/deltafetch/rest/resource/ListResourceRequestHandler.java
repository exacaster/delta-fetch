package com.exacaster.deltafetch.rest.resource;

import com.exacaster.deltafetch.configuration.ResourceConfiguration;
import com.exacaster.deltafetch.rest.ApiResponse;
import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class ListResourceRequestHandler extends ResourceRequestHandler implements
        RequestHandler<List<Map<String, Object>>> {

    public ListResourceRequestHandler(SearchService searchService, ResourceConfiguration.Resource resource) {
        super(searchService, resource);
    }

    @Override
    public Optional<ApiResponse<List<Map<String, Object>>>> handle(HttpRequest request) {
        return handleStream(request)
                .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())))
                .entrySet()
                .stream()
                .findFirst()
                .map(it -> new ApiResponse<>(it.getKey(), it.getValue()));
    }
}
