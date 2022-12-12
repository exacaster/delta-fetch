package com.exacaster.deltafetch.configuration;

import com.exacaster.deltafetch.rest.ResourceRequestHandler;
import com.exacaster.deltafetch.search.SearchService;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Factory
public class ResourceHandlerFactory {

    @Bean
    public List<ResourceRequestHandler> requestHandlers(SearchService searchService,
                                                        ResourceConfiguration configuration) {
        return configuration.getResources().stream()
                .map(resource -> new ResourceRequestHandler(searchService, resource))
                .collect(Collectors.toList());
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
