package com.exacaster.deltafetch.rest.schema;

import java.util.List;
import java.util.StringJoiner;

public class Schema {
    private final List<Field> fields;

    public Schema(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Schema.class.getSimpleName() + "[", "]")
                .add("fields=" + fields)
                .toString();
    }
}
