package com.exacaster.deltafetch.search.parquet;

import com.exacaster.deltafetch.search.ColumnValueFilter;
import com.exacaster.deltafetch.search.SearchResult;
import com.exacaster.deltafetch.search.parquet.readsupport.MapReadSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ParquetLookupReader {
    private final static Logger LOG = LoggerFactory.getLogger(ParquetLookupReader.class);

    private final String path;
    private final Configuration conf;

    public ParquetLookupReader(Configuration conf, String path) {
        this.path = path;
        this.conf = conf;
    }

    public Optional<SearchResult<Map<String, Object>>> findFirst(List<ColumnValueFilter> filters) {
        LOG.debug("Reading: {} with filters {}", path, filters);
        try (var reader = prepareReader(filters)) {
            var result = reader.read();
            if (result != null) {
                LOG.debug("Found: {}", result);
                return Optional.of(new SearchResult<>(result));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading file", e);
        }
        return Optional.empty();
    }

    private ParquetReader<Map<String, Object>> prepareReader(List<ColumnValueFilter> filters) throws IOException {
        var readerBuilder = ParquetReader
                .builder(new MapReadSupport(), new Path(path))
                .withConf(conf);
        this.toParquetPredicate(filters)
                .ifPresent(predicate -> readerBuilder.withFilter(FilterCompat.get(predicate)));
        return readerBuilder.build();
    }

    private Optional<FilterPredicate> toParquetPredicate(List<ColumnValueFilter> filters) {
        FilterPredicate result = null;
        for (var filter : filters) {
            var condition = FilterApi.eq(
                    FilterApi.binaryColumn(filter.getColumn()),
                    Binary.fromCharSequence(filter.getValue())
            );
            if (result == null) {
                result = condition;
            } else {
                result = FilterApi.and(result, condition);
            }
        }

        return Optional.ofNullable(result);
    }
}
