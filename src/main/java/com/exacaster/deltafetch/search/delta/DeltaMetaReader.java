package com.exacaster.deltafetch.search.delta;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.micronaut.cache.SyncCache;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Singleton
public class DeltaMetaReader {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaMetaReader.class);
    private final Configuration conf;
    private final SyncCache cache;
    private final ObjectMapper mapper;

    public DeltaMetaReader(Configuration conf, SyncCache cache) {
        this.conf = conf;
        this.cache = cache;
        this.mapper = new ObjectMapper()
                .enable(JsonGenerator.Feature.IGNORE_UNKNOWN)
                .disable(FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public Map<String, DeltaMeta> findCachedMeta() {
        var nativeCache = cache.getNativeCache();
        if (nativeCache instanceof Cache) {
            var caffeine = (Cache) nativeCache;
            return caffeine.asMap();
        }

        LOG.warn("Cannot get all cache values, only Caffeine supported");
        return Collections.emptyMap();
    }

    public DeltaMeta findMeta(String tablePath, boolean exact) {
        Optional<DeltaMeta> cachedStats = cache.get(tablePath, DeltaMeta.class);
        var result = cachedStats
                .map(val -> cachedOrRead(val, tablePath, exact))
                .orElseGet(() -> readLatest(tablePath));
        cache.put(tablePath, result);
        return result;
    }

    private DeltaMeta cachedOrRead(DeltaMeta cachedMeta, String tablePath, boolean exact) {
        if (!exact) {
            LOG.debug("Using cached DeltaStats, because not exact query: {}", tablePath);
            return cachedMeta;
        }
        var table = DeltaLog.forTable(conf, tablePath).snapshot();
        var version = table.getVersion();
        var schema = table.getMetadata().getSchema();
        if (cachedMeta.getVersion().equals(version)) {
            LOG.debug("Using cached DeltaStats, because version match: {}", tablePath);
            return cachedMeta;
        }
        LOG.debug("Reading newest DeltaStats from table meta: {}", tablePath);
        var stats = readFromDeltaMeta(table);
        return new DeltaMeta(version, stats, schema);
    }

    private DeltaMeta readLatest(String tablePath) {
        LOG.debug("Reading newest DeltaStats from table meta: {}", tablePath);
        var table = DeltaLog.forTable(conf, tablePath).snapshot();
        var version = table.getVersion();
        var schema = table.getMetadata().getSchema();
        var stats = readFromDeltaMeta(table);
        return new DeltaMeta(version, stats, schema);
    }

    private Map<String, FileStats> readFromDeltaMeta(Snapshot table) {
        var scan = table.scan();
        Map<String, FileStats> statList = new HashMap<>();

        try (var files = scan.getFiles()) {
            while (files.hasNext()) {
                var file = files.next();
                try {
                    var stats = mapper.readValue(file.getStats(), FileStats.class);
                    statList.put(file.getPath(), stats);
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException("Failed parsing Delta table stats", e);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading Delta table stats", e);
        }

        return statList;
    }
}
