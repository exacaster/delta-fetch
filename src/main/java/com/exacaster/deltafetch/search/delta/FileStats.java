package com.exacaster.deltafetch.search.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Map;

public class FileStats {

    private final int numRecords;
    private final Map<String, Object> minValues;
    private final Map<String, Object> maxValues;

    @JsonCreator
    public FileStats(
            @JsonProperty("numRecords") int numRecords,
            @JsonProperty("minValues") Map<String, Object> minValues,
            @JsonProperty("maxValues") Map<String, Object> maxValues) {
        this.numRecords = numRecords;
        this.minValues = minValues;
        this.maxValues = maxValues;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public Map<String, Object> getMinValues() {
        return minValues;
    }

    public Map<String, Object> getMaxValues() {
        return maxValues;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("numRecords", numRecords)
                .append("minValues", minValues)
                .append("maxValues", maxValues)
                .toString();
    }
}
