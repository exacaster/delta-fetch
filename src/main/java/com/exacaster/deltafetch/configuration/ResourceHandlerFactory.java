package com.exacaster.deltafetch.configuration;

import com.exacaster.deltafetch.rest.RequestHandler;
import com.exacaster.deltafetch.rest.Router;
import com.exacaster.deltafetch.rest.resource.ListResourceRequestHandler;
import com.exacaster.deltafetch.rest.resource.SingleResourceRequestHandler;
import com.exacaster.deltafetch.rest.schema.SchemaRequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.exacaster.deltafetch.configuration.ResponseType.LIST;

@Factory
public class ResourceHandlerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceHandlerFactory.class);

    @Bean
    public Router router(SearchService searchService,
                         DeltaMetaReader metaReader,
                         ResourceConfiguration configuration) {
        return new Router(configuration.getResources().stream()
                .flatMap(resource -> {
                    List<RequestHandler> handlers = new ArrayList<>();
                    LOG.info("Registering resource handler for {}", resource);
                    if (LIST.equals(resource.getResponseType())) {
                        handlers.add(new ListResourceRequestHandler(searchService, resource));
                    } else {
                        handlers.add(new SingleResourceRequestHandler(searchService, resource));
                    }
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
