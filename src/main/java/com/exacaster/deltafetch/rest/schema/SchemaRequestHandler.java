package com.exacaster.deltafetch.rest.schema;

import com.exacaster.deltafetch.configuration.ResourceConfiguration;
import com.exacaster.deltafetch.rest.ApiResponse;
import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import io.delta.standalone.types.StructType;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.uri.UriMatchTemplate;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.exacaster.deltafetch.rest.RequestUtils.buildDeltaPath;
import static com.exacaster.deltafetch.rest.RequestUtils.isRequestForLatestData;
import static java.util.Objects.requireNonNull;

public class SchemaRequestHandler implements RequestHandler<Schema> {
    private final DeltaMetaReader metaReader;
    private final UriMatchTemplate schemaUriMatcher;
    private final ResourceConfiguration.Resource resource;

    public SchemaRequestHandler(DeltaMetaReader metaReader, ResourceConfiguration.Resource resource) {
        this.metaReader = metaReader;
        this.schemaUriMatcher = new UriMatchTemplate(
                requireNonNull(resource.getSchemaPath(), "Schema path cannot be null")
        );
        this.resource = resource;
    }

    @Override
    public Optional<ApiResponse<Schema>> handle(HttpRequest<Void> request) {
        var path = request.getPath();
        var exact = isRequestForLatestData(request);
        return schemaUriMatcher.match(path)
                .map(info -> {
                    var deltaPath = buildDeltaPath(resource.getDeltaPath(), info);
                    var meta = metaReader.findMeta(deltaPath, exact);
                    return new ApiResponse<>(meta.getVersion(), toSchema(meta.getSchema()));
                });
    }

    private Schema toSchema(StructType schema) {
        var fields = Arrays.stream(schema.getFields())
                .map(field -> new Field(field.getName(), field.getDataType().getTypeName(), field.isNullable()))
                .collect(Collectors.toList());
        return new Schema(fields);
    }
}
