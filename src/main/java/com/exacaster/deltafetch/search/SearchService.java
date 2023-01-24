package com.exacaster.deltafetch.search;

import com.exacaster.deltafetch.search.delta.DeltaMeta;
import com.exacaster.deltafetch.search.delta.DeltaMetaReader;
import com.exacaster.deltafetch.search.delta.FileStats;
import com.exacaster.deltafetch.search.delta.PathFinder;
import com.exacaster.deltafetch.search.parquet.ParquetLookupReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Singleton
public class SearchService {

    private final DeltaMetaReader deltaMetaReader;
    private final Configuration conf;

    public SearchService(DeltaMetaReader deltaMetaReader, Configuration conf) {
        this.deltaMetaReader = deltaMetaReader;
        this.conf = conf;
    }

    public Optional<Pair<Long, Map<String, Object>>> findOne(String path, List<ColumnValueFilter> filters, boolean exact) {
        var deltaStats = findDeltaStats(path, exact);

        return findPaths(deltaStats.getFileStats(), filters)
                .parallel()
                .flatMap(filePath -> new ParquetLookupReader(conf, path + "/" + filePath)
                        .findFirst(filters).stream())
                .findFirst().map(data -> Pair.of(deltaStats.getVersion(), data));
    }

    private DeltaMeta findDeltaStats(String tablePath, boolean exact) {
        return deltaMetaReader.findMeta(tablePath, exact);
    }

    private Stream<String> findPaths(Map<String, FileStats> fileStats, List<ColumnValueFilter> filters) {
        return new PathFinder(fileStats).findCandidatePaths(filters);
    }
}
