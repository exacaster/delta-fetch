package com.exacaster.deltafetch.search;

import com.exacaster.deltafetch.search.delta.DeltaStatsReader;
import com.exacaster.deltafetch.search.delta.PathFinder;
import com.exacaster.deltafetch.search.parquet.ParquetLookupReader;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Singleton
public class SearchService {

    private final DeltaStatsReader deltaStatsReader;
    private final Configuration conf;

    public SearchService(DeltaStatsReader deltaStatsReader, Configuration conf) {
        this.deltaStatsReader = deltaStatsReader;
        this.conf = conf;
    }

    public Optional<SearchResult<Map<String, Object>>> findOne(String path, List<ColumnValueFilter> filters, boolean exact) {
        return findPaths(path, filters, exact)
                .parallel()
                .flatMap(filePath -> new ParquetLookupReader(conf, path + "/" + filePath)
                        .findFirst(filters).stream())
                .findFirst();
    }

    private Stream<String> findPaths(String tablePath, List<ColumnValueFilter> filters, boolean exact) {
        return new PathFinder(deltaStatsReader.findStats(tablePath, exact).getFileStats())
                .findCandidatePaths(filters);
    }
}
