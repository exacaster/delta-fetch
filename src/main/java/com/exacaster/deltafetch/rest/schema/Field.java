package com.exacaster.deltafetch.rest.schema;

import java.util.StringJoiner;

public class Field {
    private final String name;
    private final String type;
    private final boolean nullable;

    public Field(String name, String type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Field.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("type='" + type + "'")
                .add("nullable=" + nullable)
                .toString();
    }
}
