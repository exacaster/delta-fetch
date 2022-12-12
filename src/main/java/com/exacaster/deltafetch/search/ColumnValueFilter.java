package com.exacaster.deltafetch.search;

import java.util.StringJoiner;

public class ColumnValueFilter {
    private final String column;
    private final String value;

    public ColumnValueFilter(String column, String value) {
        this.column = column;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ColumnValueFilter.class.getSimpleName() + "[", "]")
                .add("column='" + column + "'")
                .add("value='" + value + "'")
                .toString();
    }
}
