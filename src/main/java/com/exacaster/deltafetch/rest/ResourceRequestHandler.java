package com.exacaster.deltafetch.rest;

import com.exacaster.deltafetch.configuration.ResourceConfiguration;
import com.exacaster.deltafetch.search.ColumnValueFilter;
import com.exacaster.deltafetch.search.SearchResult;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.uri.UriMatchInfo;
import io.micronaut.http.uri.UriMatchTemplate;
import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;

public class ResourceRequestHandler {
    private final SearchService searchService;
    private final ResourceConfiguration.Resource resource;
    private final UriMatchTemplate template;

    public ResourceRequestHandler(SearchService searchService, ResourceConfiguration.Resource resource) {
        this.searchService = searchService;
        this.resource = resource;
        this.template = new UriMatchTemplate(resource.getPath());
    }

    public Optional<SearchResult> handle(HttpRequest request) {
        var path = request.getPath();
        var exact = request.getParameters().get("exact", Boolean.class).filter(val -> val).isPresent();
        return template.match(path)
                .flatMap(info -> {
                    var deltaPath = buildDeltaPath(info);
                    var filters = buildFilters(info);
                    return searchService.findOne(deltaPath, filters, exact);
                });
    }

    private String buildDeltaPath(UriMatchInfo info) {
        return StrSubstitutor.replace(resource.getDeltaPath(), info.getVariableValues(), "{", "}");
    }

    private List<ColumnValueFilter> buildFilters(UriMatchInfo info) {
        return resource.getFilterVariables().stream()
                .map(variable -> new ColumnValueFilter(variable.getColumn(),
                        ofNullable(variable.getPathVariable()).map(key -> info.getVariableValues().get(key))
                                .map(Object::toString)
                                .orElse(variable.getStaticValue()))).collect(Collectors.toList());
    }
}
