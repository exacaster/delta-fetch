package com.exacaster.deltafetch.rest.resource;

import com.exacaster.deltafetch.configuration.ResourceConfiguration;
import com.exacaster.deltafetch.rest.APIResponse;
import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.search.ColumnValueFilter;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.uri.UriMatchInfo;
import io.micronaut.http.uri.UriMatchTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.exacaster.deltafetch.rest.RequestUtils.buildDeltaPath;
import static com.exacaster.deltafetch.rest.RequestUtils.isRequestForLatestData;
import static java.util.Optional.ofNullable;

public class ResourceRequestHandler implements RequestHandler<Map<String, Object>> {
    private final SearchService searchService;
    private final ResourceConfiguration.Resource resource;
    private final UriMatchTemplate resourceTemplate;

    public ResourceRequestHandler(SearchService searchService, ResourceConfiguration.Resource resource) {
        this.searchService = searchService;
        this.resource = resource;
        this.resourceTemplate = new UriMatchTemplate(resource.getPath());
    }

    public Optional<APIResponse<Map<String, Object>>> handle(HttpRequest request) {
        var path = request.getPath();
        var exact = isRequestForLatestData(request);
        return resourceTemplate.match(path)
                .flatMap(info -> {
                    var deltaPath = buildDeltaPath(resource.getDeltaPath(), info);
                    var filters = buildFilters(info);
                    return searchService.findOne(deltaPath, filters, exact)
                            .map(result -> new APIResponse<>(result.getKey(), result.getValue()));
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
                                .orElse(variable.getStaticValue()))).collect(Collectors.toList());
    }
}
