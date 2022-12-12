package com.exacaster.deltafetch.search.parquet.readsupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class MapReadSupport extends ReadSupport<Map<String, Object>> {
    @Override
    public ReadContext init(
            Configuration configuration, Map<String, String> keyValueMetaData,
            org.apache.parquet.schema.MessageType fileSchema) {
        String partialSchemaString = configuration.get(org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA);
        org.apache.parquet.schema.MessageType requestedProjection = getSchemaForRead(fileSchema, partialSchemaString);
        return new org.apache.parquet.hadoop.api.ReadSupport.ReadContext(requestedProjection);
    }

    @Override
    public RecordMaterializer<Map<String, Object>> prepareForRead(Configuration configuration,
                                                                  Map<String, String> keyValueMetaData, MessageType fileSchema,
                                                                  ReadContext readContext) {
        return new MapRecordMaterializer(readContext.getRequestedSchema());
    }
}
