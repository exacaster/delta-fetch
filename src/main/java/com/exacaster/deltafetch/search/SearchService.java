package com.exacaster.deltafetch.search;

import com.exacaster.deltafetch.search.delta.DeltaMeta;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import com.exacaster.deltafetch.search.delta.FileStats;
import com.exacaster.deltafetch.search.delta.PathFinder;
import com.exacaster.deltafetch.search.parquet.ParquetLookupReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final long FILE_READ_TIMEOUT_SECONDS = 3;

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

        BlockingQueue<Map<String, Object>> results = new ArrayBlockingQueue<>(limit);
        AtomicInteger completedFiles = new AtomicInteger(0);

        List<CompletableFuture<Void>> futures = paths.stream()
            .map(filePath -> readFileAsync(path, filePath, filters, results, completedFiles))
            .collect(Collectors.toList());

        waitForCompletion(futures, results, completedFiles, paths.size());

        LOG.debug("Read {} total results from {} completed files out of {} total files",
            results.size(), completedFiles.get(), paths.size());

        return results.stream()
            .map(data -> Pair.of(deltaStats.getVersion(), data));
    }

    private DeltaMeta findDeltaStats(String tablePath, boolean exact) {
        return deltaMetaReader.findMeta(tablePath, exact);
    }

    private Stream<String> findPaths(Map<String, FileStats> fileStats, List<ColumnValueFilter> filters) {
        return new PathFinder(fileStats).findCandidatePaths(filters);
    }

    private CompletableFuture<Void> readFileAsync(
            String tablePath,
            String filePath,
            List<ColumnValueFilter> filters,
            BlockingQueue<Map<String, Object>> results,
            AtomicInteger completedFiles) {

        return CompletableFuture.runAsync(() -> {
            if (Thread.currentThread().isInterrupted() || results.remainingCapacity() == 0) {
                LOG.debug("Skipping {} - limit already reached or thread interrupted", filePath);
                return;
            }

            readFileAndCollectResults(tablePath, filePath, filters, results.remainingCapacity(),
                                     results);
        }, executorService)
        .orTimeout(FILE_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .whenComplete((result, throwable) -> {
            completedFiles.incrementAndGet();

            if (throwable != null) {
                if (throwable instanceof java.util.concurrent.TimeoutException) {
                    LOG.warn("Timeout reading {} after {} seconds", filePath, FILE_READ_TIMEOUT_SECONDS);
                } else {
                    LOG.error("Error reading {}: {}", filePath, throwable.getMessage());
                }
            }
        });
    }

    private void readFileAndCollectResults(
            String tablePath,
            String filePath,
            List<ColumnValueFilter> filters,
            int remainingCapacity,
            BlockingQueue<Map<String, Object>> results) {

        var reader = new ParquetLookupReader(conf, tablePath + "/" + filePath);
        try (Stream<Map<String, Object>> stream = reader.find(filters, remainingCapacity)) {
            stream.forEach(record -> {
                if (Thread.currentThread().isInterrupted()) {
                    throw new TaskInterruptedException();
                }

                if (!results.offer(record)) {
                    throw new LimitReachedException();
                }
            });
        } catch (LimitReachedException | TaskInterruptedException e) {
            LOG.debug("Reading {} stopped: {}", filePath, e.getMessage());
        } catch (Exception e) {
            LOG.error("Error reading {}: {}", filePath, e.getMessage());
        }
    }

    private void waitForCompletion(
            List<CompletableFuture<Void>> futures,
            BlockingQueue<Map<String, Object>> results,
            AtomicInteger completedFiles,
            int totalFiles) {

        long startTime = System.currentTimeMillis();
        long timeoutMillis = SEARCH_TIMEOUT_SECONDS * 1000;

        try {
            while (results.remainingCapacity() > 0 && completedFiles.get() < totalFiles) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= timeoutMillis) {
                    LOG.warn("Search timeout after {} seconds, cancelling remaining reads", SEARCH_TIMEOUT_SECONDS);
                    break;
                }
                Thread.sleep(10);
            }

            if (results.remainingCapacity() == 0) {
                futures.forEach(future -> future.cancel(true));
                LOG.debug("Cancelled pending file reads after reaching limit");
            }
        } catch (InterruptedException e) {
            LOG.error("Search interrupted", e);
            Thread.currentThread().interrupt();
            futures.forEach(future -> future.cancel(true));
        }
    }

    private static class LimitReachedException extends RuntimeException {
        public LimitReachedException() {
            super("Limit reached");
        }
    }

    private static class TaskInterruptedException extends RuntimeException {
        public TaskInterruptedException() {
            super("Task interrupted");
        }
    }
}
