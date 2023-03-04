package com.exacaster.deltafetch.search.delta;

import io.delta.standalone.types.StructType;

import java.util.Map;
import java.util.StringJoiner;

public class DeltaMeta {
    private final Long version;
    private final Map<String, FileStats> fileStats;
    private final StructType schema;

    public DeltaMeta(Long version, Map<String, FileStats> fileStats, StructType schema) {
        this.version = version;
        this.fileStats = fileStats;
        this.schema = schema;
    }

    public Long getVersion() {
        return version;
    }

    public StructType getSchema() {
        return schema;
    }

    public Map<String, FileStats> getFileStats() {
        return fileStats;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DeltaMeta.class.getSimpleName() + "[", "]")
                .add("version=" + version)
                .add("fileStats=" + fileStats)
                .add("schema=" + schema)
                .toString();
    }
}
