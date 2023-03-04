package com.exacaster.deltafetch.rest.resource;

import com.exacaster.deltafetch.configuration.ResourceConfiguration.Resource;
import com.exacaster.deltafetch.search.ColumnValueFilter;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.uri.UriMatchInfo;
import io.micronaut.http.uri.UriMatchTemplate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exacaster.deltafetch.rest.RequestUtils.buildDeltaPath;
import static com.exacaster.deltafetch.rest.RequestUtils.isRequestForLatestData;
import static java.util.Optional.ofNullable;

public class RequestLookup {
    private final SearchService searchService;
    private final Resource resource;
    private final UriMatchTemplate resourceTemplate;

    public RequestLookup(SearchService searchService, Resource resource) {
        this.searchService = searchService;
        this.resource = resource;
        this.resourceTemplate = new UriMatchTemplate(resource.getPath());
    }

    protected Stream<Pair<Long, Map<String, Object>>> handleStream(HttpRequest request, int limit) {
        var path = request.getPath();
        var exact = isRequestForLatestData(request);
        return resourceTemplate.match(path).stream()
                .flatMap(info -> {
                    var deltaPath = buildDeltaPath(resource.getDeltaPath(), info);
                    var filters = buildFilters(info);
                    return searchService.find(deltaPath, filters, exact, limit);
                });
    }

    private List<ColumnValueFilter> buildFilters(UriMatchInfo info) {
        return resource.getFilterVariables().stream()
                .map(variable -> new ColumnValueFilter(
                        ofNullable(variable.getPathColumn())
                                .map(pathVar -> info.getVariableValues().get(pathVar))
                                .map(Object::toString)
                                .orElse(variable.getColumn()),
                        ofNullable(variable.getPathVariable())
                                .map(key -> info.getVariableValues().get(key))
                                .map(Object::toString)
                                .orElse(variable.getStaticValue())))
                .collect(Collectors.toList());
    }
}
