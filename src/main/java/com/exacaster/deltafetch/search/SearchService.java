package com.exacaster.deltafetch.search;

import com.exacaster.deltafetch.search.delta.DeltaMeta;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import com.exacaster.deltafetch.search.delta.FileStats;
import com.exacaster.deltafetch.search.delta.PathFinder;
import com.exacaster.deltafetch.search.parquet.ParquetLookupReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class SearchService {
    private static final Logger LOG = LoggerFactory.getLogger(SearchService.class);
    private static final long SEARCH_TIMEOUT_SECONDS = 30;

    private final DeltaMetaReader deltaMetaReader;
    private final Configuration conf;
    private final ExecutorService executorService;

    public SearchService(DeltaMetaReader deltaMetaReader, Configuration conf,
                        @Named("parquet-reader") ExecutorService executorService) {
        this.deltaMetaReader = deltaMetaReader;
        this.conf = conf;
        this.executorService = executorService;
    }

    public Stream<Pair<Long, Map<String, Object>>> find(String path, List<ColumnValueFilter> filters,
            boolean exact, int limit) {
        var deltaStats = findDeltaStats(path, exact);
        List<String> paths = findPaths(deltaStats.getFileStats(), filters).collect(Collectors.toList());

        if (paths.isEmpty()) {
            return Stream.empty();
        }

        List<CompletableFuture<List<Map<String, Object>>>> futures = paths.stream()
            .map(filePath ->
                CompletableFuture.supplyAsync(() -> {
                    try {
                        var reader = new ParquetLookupReader(conf, path + "/" + filePath);
                        List<Map<String, Object>> fileResults = reader.find(filters, limit)
                            .collect(Collectors.toList());
                        if (!fileResults.isEmpty()) {
                            LOG.debug("Read {} results from {}", fileResults.size(), filePath);
                        }
                        return fileResults;
                    } catch (Exception e) {
                        LOG.error("Error reading {}: {}", filePath, e.getMessage());
                        return Collections.<Map<String, Object>>emptyList();
                    }
                }, executorService)
            )
            .collect(Collectors.toList());

        try {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
            );
            allFutures.get(SEARCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.error("Parallel search timed out after {} seconds for {} files", SEARCH_TIMEOUT_SECONDS, paths.size());
            throw new RuntimeException("Search operation timed out", e);
        } catch (Exception e) {
            LOG.error("Parallel search failed", e);
            throw new RuntimeException("Search operation failed", e);
        }

        List<Map<String, Object>> results = futures.stream()
            .map(CompletableFuture::join)
            .flatMap(List::stream)
            .collect(Collectors.toList());

        LOG.debug("Read {} total results from {} files, returning first {}",
            results.size(), paths.size(), Math.min(results.size(), limit));

        return results.stream()
            .limit(limit)
            .map(data -> Pair.of(deltaStats.getVersion(), data));
    }

    private DeltaMeta findDeltaStats(String tablePath, boolean exact) {
        return deltaMetaReader.findMeta(tablePath, exact);
    }

    private Stream<String> findPaths(Map<String, FileStats> fileStats, List<ColumnValueFilter> filters) {
        return new PathFinder(fileStats).findCandidatePaths(filters);
    }
}
