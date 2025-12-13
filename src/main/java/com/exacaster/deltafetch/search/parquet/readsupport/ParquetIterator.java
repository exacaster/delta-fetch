package com.exacaster.deltafetch.search.parquet.readsupport;

import com.google.common.collect.AbstractIterator;
import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.parquet.hadoop.ParquetReader;

public class ParquetIterator<T> extends AbstractIterator<T> {

    private final ParquetReader<T> reader;

    public ParquetIterator(ParquetReader<T> reader) {
        this.reader = reader;
    }

    @Override
    protected T computeNext() {
        try {
            T record = reader.read();
            return (record == null) ? endOfData() : record;
        } catch (InterruptedIOException e) {
            // Stream was cancelled (e.g., limit reached), treat as end of data
            return endOfData();
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading file", e);
        }
    }
}
