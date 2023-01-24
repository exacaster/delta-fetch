package com.exacaster.deltafetch.configuration;

import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.rest.Router;
import com.exacaster.deltafetch.rest.resource.ResourceRequestHandler;
import com.exacaster.deltafetch.rest.schema.SchemaRequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Factory
public class ResourceHandlerFactory {

    @Bean
    public Router router(SearchService searchService,
                         DeltaMetaReader metaReader,
                         ResourceConfiguration configuration) {
        return new Router(configuration.getResources().stream()
                .flatMap(resource -> {
                    List<RequestHandler> handlers = new ArrayList<>();
                    handlers.add(new ResourceRequestHandler(searchService, resource));
                    if (resource.getSchemaPath() != null) {
                        handlers.add(new SchemaRequestHandler(metaReader, resource));
                    }
                    return handlers.stream();
                })
                .collect(Collectors.toList()));
    }

    @Bean
    public Configuration hadoopConfiguration(ResourceConfiguration configuration) {
        var conf = new Configuration();
        if (configuration.getHadoopProps() != null) {
            configuration.getHadoopProps().forEach(conf::set);
        }
        return conf;
    }
}
