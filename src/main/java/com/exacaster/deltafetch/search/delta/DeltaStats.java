package com.exacaster.deltafetch.search.delta;

import java.util.Map;

public class DeltaStats {
    private final Long version;
    private final Map<String, FileStats> fileStats;

    public DeltaStats(Long version, Map<String, FileStats> fileStats) {
        this.version = version;
        this.fileStats = fileStats;
    }

    public Long getVersion() {
        return version;
    }

    public Map<String, FileStats> getFileStats() {
        return fileStats;
    }
}
