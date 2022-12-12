package com.exacaster.deltafetch.search.delta;

import com.exacaster.deltafetch.search.ColumnValueFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PathFinder {

    private final Map<String, FileStats> filePaths;

    public PathFinder(Map<String, FileStats> filePaths) {
        this.filePaths = filePaths;
    }

    public Stream<String> findCandidatePaths(List<ColumnValueFilter> filters) {
        return filePaths.entrySet().stream()
                .filter((entry) -> {
                    var val = entry.getValue();
                    return filterApplies(val, filters);
                }).map(Map.Entry::getKey);
    }

    private boolean filterApplies(FileStats val, List<ColumnValueFilter> filters) {
        return filters.stream().allMatch(filter -> inRange(
                filter.getValue(),
                val.getMinValues().get(filter.getColumn()).toString(),
                val.getMaxValues().get(filter.getColumn()).toString()
        ));
    }

    private boolean inRange(String string, String from, String to) {
        return string.compareTo(from) >= 0 && string.compareTo(to) <= 0;
    }
}
