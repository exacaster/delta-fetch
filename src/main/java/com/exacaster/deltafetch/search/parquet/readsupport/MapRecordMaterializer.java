package com.exacaster.deltafetch.search.parquet.readsupport;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class MapRecordMaterializer extends RecordMaterializer<Map<String, Object>> {

    private final MapConverter root;

    public MapRecordMaterializer(MessageType schema) {
        this.root = new MapConverter(schema);
    }

    @Override
    public Map<String, Object> getCurrentRecord() {
        return this.root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
