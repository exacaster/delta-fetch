package com.exacaster.deltafetch.search.delta;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.micronaut.cache.SyncCache;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Singleton
public class DeltaMetaReader {
    private final static Logger LOG = LoggerFactory.getLogger(DeltaMetaReader.class);
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

    public DeltaMeta findMeta(String tablePath, boolean exact) {
        Optional<DeltaMeta> cachedStats = cache.get(tablePath, DeltaMeta.class);
        var result = cachedStats.map(val -> {
            if (!exact) {
                LOG.debug("Using cached DeltaStats, because not exact query: {}", tablePath);
                return val;
            }
            var table = DeltaLog.forTable(conf, tablePath).snapshot();
            var version = table.getVersion();
            var schema = table.getMetadata().getSchema();
            if (val.getVersion().equals(version)) {
                LOG.debug("Using cached DeltaStats, because version match: {}", tablePath);
                return val;
            }
            LOG.debug("Reading newest DeltaStats from table meta: {}", tablePath);
            var stats = readFromDeltaMeta(table);
            return new DeltaMeta(version, stats, schema);
        }).orElseGet(() -> {
            LOG.debug("Reading newest DeltaStats from table meta: {}", tablePath);
            var table = DeltaLog.forTable(conf, tablePath).snapshot();
            var version = table.getVersion();
            var schema = table.getMetadata().getSchema();
            var stats = readFromDeltaMeta(table);
            return new DeltaMeta(version, stats, schema);
        });
        cache.put(tablePath, result);
        return result;
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
