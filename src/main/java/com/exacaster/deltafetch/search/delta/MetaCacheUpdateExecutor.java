package com.exacaster.deltafetch.search.delta;

import com.exacaster.deltafetch.configuration.ResourceConfiguration;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
@Requires(property = "app.cache-update-interval")
public class MetaCacheUpdateExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(MetaCacheUpdateExecutor.class);

    private final DeltaMetaReader metaReader;
    private final Set<String> staticResourcePaths;

    public MetaCacheUpdateExecutor(DeltaMetaReader metaReader, ResourceConfiguration config) {
        this.metaReader = metaReader;
        this.staticResourcePaths = config.getResources().stream()
                .map(ResourceConfiguration.Resource::getDeltaPath)
                .filter(path -> {
                    var result = !path.contains("{");
                    if (!result) {
                        LOG.info("Path {} cannot be initially warmed up for cache,"
                                + " because it contains dynamic variables.", path);
                    }
                    return result;
                }).collect(Collectors.toSet());
    }

    @Scheduled(initialDelay = "0s", fixedRate = "${app.cache-update-interval}")
    public void warmupResources() {
        LOG.info("Running cache update");
        Set<String> paths = new HashSet<>(staticResourcePaths);
        paths.addAll(metaReader.findCachedMeta().keySet());

        paths.forEach(tablePath -> {
            LOG.debug("Updating memorised meta for {}", tablePath);
            metaReader.findMeta(tablePath, true);
        });
    }
}
