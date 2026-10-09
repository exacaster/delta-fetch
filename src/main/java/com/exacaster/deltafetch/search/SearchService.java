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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
    private final Duration fileReadTimeout;
    private final Duration searchTimeout;

    @Inject
    public SearchService(DeltaMetaReader deltaMetaReader, Configuration conf,
                        @Named("parquet-reader") ExecutorService executorService) {
        this(deltaMetaReader, conf, executorService, FILE_READ_TIMEOUT, SEARCH_TIMEOUT);
    }

    SearchService(DeltaMetaReader deltaMetaReader, Configuration conf, ExecutorService executorService,
                  Duration fileReadTimeout, Duration searchTimeout) {
        this.deltaMetaReader = deltaMetaReader;
        this.conf = conf;
        this.scheduler = Schedulers.fromExecutorService(executorService);
        this.fileReadTimeout = fileReadTimeout;
        this.searchTimeout = searchTimeout;
    }

    public Stream<Pair<Long, Map<String, Object>>> find(String path, List<ColumnValueFilter> filters,
            boolean exact, int limit) {
        var deltaStats = findDeltaStats(path, exact);
        List<String> paths = findPaths(deltaStats.getFileStats(), filters).collect(Collectors.toList());

        if (paths.isEmpty()) {
            return Stream.empty();
        }

        LOG.debug("Starting search across {} files with limit {}", paths.size(), limit);

        var unreadFiles = new AtomicInteger();
        List<Map<String, Object>> results = Flux.fromIterable(paths)
            .flatMap(filePath -> readFile(path, filePath, filters, limit)
                .onErrorResume(TimeoutException.class, e -> {
                    LOG.warn("Timeout reading file [file={}, timeout={}]", filePath, fileReadTimeout);
                    unreadFiles.incrementAndGet();
                    return Mono.empty();
                })
                .onErrorResume(e -> {
                    LOG.error("Error reading file [file={}]", filePath, e);
                    unreadFiles.incrementAndGet();
                    return Mono.empty();
                })
                .flatMapIterable(Function.identity()), MAX_CONCURRENCY)
            .take(limit)
            .collectList()
            .timeout(searchTimeout)
            .onErrorMap(TimeoutException.class, e -> new IncompleteSearchException(
                String.format("Search did not finish within %s", searchTimeout)))
            .block();

        LOG.debug("Found {} results (searched {} files)", results.size(), paths.size());

        if (results.size() < limit && unreadFiles.get() > 0) {
            LOG.warn("Search incomplete, some files were not read [table={}, unreadFiles={}, files={}]",
                path, unreadFiles.get(), paths.size());
            throw new IncompleteSearchException(
                String.format("%d of %d files could not be read", unreadFiles.get(), paths.size()));
        }

        return results.stream()
            .map(data -> Pair.of(deltaStats.getVersion(), data));
    }

    private DeltaMeta findDeltaStats(String tablePath, boolean exact) {
        return deltaMetaReader.findMeta(tablePath, exact);
    }

    private Stream<String> findPaths(Map<String, FileStats> fileStats, List<ColumnValueFilter> filters) {
        return new PathFinder(fileStats).findCandidatePaths(filters);
    }

    private Mono<List<Map<String, Object>>> readFile(
            String tablePath,
            String filePath,
            List<ColumnValueFilter> filters,
            int limit) {

        // timeout before subscribeOn: the timer starts when a reader thread picks the file up,
        // so waiting for a free thread does not count against the file read timeout
        return Mono.fromCallable(() -> {
                LOG.debug("Reading file: {} with limit: {}", filePath, limit);
                try (var records = new ParquetLookupReader(conf, tablePath + "/" + filePath).find(filters, limit)) {
                    return records.collect(Collectors.toList());
                }
            })
            .timeout(fileReadTimeout)
            .subscribeOn(scheduler);
    }
}
