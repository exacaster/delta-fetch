package com.exacaster.deltafetch.search;

import com.exacaster.deltafetch.search.delta.DeltaMeta;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import com.exacaster.deltafetch.search.delta.FileStats;
import com.exacaster.deltafetch.search.delta.PathFinder;
import com.exacaster.deltafetch.search.parquet.ParquetLookupReader;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Singleton
public class SearchService {
    private static final Logger LOG = LoggerFactory.getLogger(SearchService.class);
    private static final long SEARCH_TIMEOUT_SECONDS = 30;
    private static final long FILE_READ_TIMEOUT_SECONDS = 3;
    private static final int MAX_CONCURRENCY = 5;


    private final DeltaMetaReader deltaMetaReader;
    private final Configuration conf;
    private final Scheduler scheduler;

    public SearchService(DeltaMetaReader deltaMetaReader, Configuration conf,
                        @Named("parquet-reader") ExecutorService executorService) {
        this.deltaMetaReader = deltaMetaReader;
        this.conf = conf;
        this.scheduler = Schedulers.fromExecutorService(executorService);
    }

    public Stream<Pair<Long, Map<String, Object>>> find(String path, List<ColumnValueFilter> filters,
            boolean exact, int limit) {
        var deltaStats = findDeltaStats(path, exact);
        List<String> paths = findPaths(deltaStats.getFileStats(), filters).collect(Collectors.toList());

        if (paths.isEmpty()) {
            return Stream.empty();
        }

        LOG.debug("Starting search across {} files with limit {}", paths.size(), limit);

        List<Map<String, Object>> results = Flux.fromIterable(paths)
            .flatMap(filePath -> readFile(path, filePath, filters, limit)
                .timeout(Duration.ofSeconds(FILE_READ_TIMEOUT_SECONDS))
                .onErrorResume(e -> {
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        LOG.warn("Timeout reading {} after {} seconds", filePath, FILE_READ_TIMEOUT_SECONDS);
                    } else {
                        LOG.error("Error reading {}: {}", filePath, e.getMessage());
                    }
                    return Flux.empty();
                }), MAX_CONCURRENCY)
            .take(limit)
            .timeout(Duration.ofSeconds(SEARCH_TIMEOUT_SECONDS))
            .collectList()
            .block();

        if (results == null) {
            LOG.warn("Search timed out or was interrupted for path: {}", path);
            return Stream.empty();
        }

        LOG.debug("Read {} total results from {} files", results.size(), paths.size());

        return results.stream()
            .map(data -> Pair.of(deltaStats.getVersion(), data));
    }

    private DeltaMeta findDeltaStats(String tablePath, boolean exact) {
        return deltaMetaReader.findMeta(tablePath, exact);
    }

    private Stream<String> findPaths(Map<String, FileStats> fileStats, List<ColumnValueFilter> filters) {
        return new PathFinder(fileStats).findCandidatePaths(filters);
    }

    private Flux<Map<String, Object>> readFile(
            String tablePath,
            String filePath,
            List<ColumnValueFilter> filters,
            int limit) {

        return Flux.defer(() -> {
            LOG.debug("Reading file: {} with limit: {}", filePath, limit);
            var reader = new ParquetLookupReader(conf, tablePath + "/" + filePath);
            try {
                Stream<Map<String, Object>> stream = reader.find(filters, limit);
                return Flux.fromStream(stream)
                    .doFinally(signal -> {
                        try {
                            stream.close();
                            LOG.debug("Closed stream for {} with signal: {}", filePath, signal);
                        } catch (Exception e) {
                            LOG.warn("Error closing stream for {}: {}", filePath, e.getMessage());
                        }
                    });
            } catch (Exception e) {
                LOG.error("Error opening file {}: {}", filePath, e.getMessage());
                return Flux.empty();
            }
        }).subscribeOn(scheduler);
    }
}
