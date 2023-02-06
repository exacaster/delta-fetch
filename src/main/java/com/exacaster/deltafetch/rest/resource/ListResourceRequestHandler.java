package com.exacaster.deltafetch.rest.resource;

import static java.util.Optional.ofNullable;

import com.exacaster.deltafetch.configuration.ResourceConfiguration.Resource;
import com.exacaster.deltafetch.rest.ApiResponse;
import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Pair;

public class ListResourceRequestHandler extends ResourceRequestHandler implements
        RequestHandler<List<Map<String, Object>>> {

    private final Range<Integer> limitRange;

    public ListResourceRequestHandler(SearchService searchService, Resource resource) {
        super(searchService, resource);
        this.limitRange = Range.between(1, ofNullable(resource.getMaxResults()).orElse(100));
    }

    @Override
    public Optional<ApiResponse<List<Map<String, Object>>>> handle(HttpRequest request) {
        return handleStream(request, getLimit(request))
                .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toList())))
                .entrySet()
                .stream()
                .findFirst()
                .map(it -> new ApiResponse<>(it.getKey(), it.getValue()));
    }

    private int getLimit(HttpRequest request) {
        return request.getParameters().get("limit", Integer.class)
                .filter(limitRange::contains)
                .orElseGet(limitRange::getMaximum);
    }
}
