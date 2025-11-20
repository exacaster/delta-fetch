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
import java.util.concurrent.TimeoutException;
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
    private static final Duration SEARCH_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration FILE_READ_TIMEOUT = Duration.ofSeconds(3);
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
                .timeout(FILE_READ_TIMEOUT)
                .onErrorResume(TimeoutException.class, e -> {
                    LOG.warn("Timeout reading {} after {} seconds", filePath, FILE_READ_TIMEOUT.getSeconds());
                    return Flux.empty();
                })
                .onErrorResume(e -> {
                    LOG.error("Error reading {}", filePath, e);
                    return Flux.empty();
                }), MAX_CONCURRENCY)
            .timeout(SEARCH_TIMEOUT)
            .take(limit)
            .collectList()
            .block();

        LOG.debug("Found {} results (searched {} files)", results.size(), paths.size());

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

        return Flux.using(
            () -> {
                LOG.debug("Reading file: {} with limit: {}", filePath, limit);
                var reader = new ParquetLookupReader(conf, tablePath + "/" + filePath);
                return reader.find(filters, limit);
            },
            Flux::fromStream,
            Stream::close
        ).subscribeOn(scheduler);
    }
}
