package com.exacaster.deltafetch.search.delta;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import io.delta.kernel.Scan;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.parquet.ParquetStatsReader;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.skipping.StatsSchemaHelper;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.micronaut.cache.SyncCache;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

@Singleton
public class DeltaMetaReader {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaMetaReader.class);
    private final Configuration conf;
    private final SyncCache cache;
    private final ObjectMapper mapper;
    private final Engine engine;

    public DeltaMetaReader(Configuration conf, SyncCache cache) {
        this.conf = conf;
        this.cache = cache;
        this.mapper = new ObjectMapper()
            .enable(JsonGenerator.Feature.IGNORE_UNKNOWN)
            .disable(FAIL_ON_UNKNOWN_PROPERTIES);
        this.engine = DefaultEngine.create(conf);
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

        var table = Table.forPath(engine, tablePath);
        var snapshot = table.getLatestSnapshot(engine);
        var version = snapshot.getVersion(engine);
        var schema = snapshot.getSchema(engine);
        if (cachedMeta.getVersion().equals(version)) {
            LOG.debug("Using cached DeltaStats, because version match: {}", tablePath);
            return cachedMeta;
        }
        LOG.debug("Reading newest DeltaStats from table meta: {}", tablePath);
        var stats = readFromDeltaMeta(snapshot);
        return new DeltaMeta(version, stats, schema);
    }

    private DeltaMeta readLatest(String tablePath) {
        LOG.debug("Reading newest DeltaStats from table meta: {}", tablePath);
        var table = Table.forPath(engine, tablePath);
        var snapshot = table.getLatestSnapshot(engine);
        var version = snapshot.getVersion(engine);
        var schema = snapshot.getSchema(engine);
        var stats = readFromDeltaMeta(snapshot);
        return new DeltaMeta(version, stats, schema);
    }

    private Map<String, FileStats> readFromDeltaMeta(Snapshot snapshot) {

        var scan = snapshot.getScanBuilder(engine).build();

        Map<String, FileStats> statList = new HashMap<>();
        try (var files = scan.getScanFiles(engine)) {
            Row scanStateRow = scan.getScanState(engine);
            while (files.hasNext()) {
                var file = files.next();

                StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow);

                var bb = StatsSchemaHelper.getStatsSchema(physicalReadSchema);

                LOG.info("Stats schema: {}", bb);


                try (CloseableIterator<Row> scanFileRows = file.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
                        CloseableIterator<ColumnarBatch> physicalDataIter =
                            engine.getParquetHandler().readParquetFiles(
                                singletonCloseableIterator(fileStatus),
                                physicalReadSchema,
                                Optional.empty() /* optional predicate the connector can apply to filter data from the reader */
                            );
                        try (
                            CloseableIterator<FilteredColumnarBatch> transformedData =
                                Scan.transformPhysicalData(
                                    engine,
                                    scanStateRow,
                                    scanFileRow,
                                    physicalDataIter)) {
                            while (transformedData.hasNext()) {
                                FilteredColumnarBatch logicalData = transformedData.next();
                                ColumnarBatch dataBatch = logicalData.getData();

                                Optional<ColumnVector> selectionVector = logicalData.getSelectionVector();

                                // access the data for the column at ordinal 0
                                ColumnVector column0 = dataBatch.getColumnVector(0);
                                for (int rowIndex = 0; rowIndex < column0.getSize(); rowIndex++) {
                                    // check if the row is selected or not
                                    if (!selectionVector.isPresent() || // there is no selection vector, all records are selected
                                        (!selectionVector.get().isNullAt(rowIndex) && selectionVector.get().getBoolean(rowIndex)))  {
                                        // Assuming the column type is String.
                                        // If it is a different type, call the relevant function on the `ColumnVector`
                                        var pr = column0.getString(rowIndex);
                                        System.out.println(pr);
                                    }
                                }

                                LOG.info("aa: {}", dataBatch);
                            }
                        }
                    }
                }
                var rows = file.getData().getRows();
                while (rows.hasNext()) {
                    var row = rows.next();
                    var t = row.getArray(0);
                    LOG.info("A: {}", t);
                }
                LOG.info("Schema: {}", file.getData().getSchema());

            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading Delta table stats", e);
        }

        return statList;
    }

}
