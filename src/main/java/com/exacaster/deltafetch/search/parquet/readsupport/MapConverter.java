package com.exacaster.deltafetch.search.parquet.readsupport;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.util.*;

import static java.util.Optional.of;

final class MapConverter extends GroupConverter {
    private final Converter[] converters;
    private final String name;
    private final MapConverter parent;
    protected Map<String, Object> record;

    public MapConverter(GroupType schema) {
        this(schema, null, null);
    }

    public MapConverter(GroupType schema, String name, MapConverter parent) {
        this.converters = new Converter[schema.getFieldCount()];
        this.parent = parent;
        this.name = name;

        int i = 0;
        for (Type field : schema.getFields()) {
            converters[i++] = createConverter(field);
        }
    }

    private Converter createConverter(Type field) {
        LogicalTypeAnnotation ltype = field.getLogicalTypeAnnotation();

        if (field.isPrimitive()) {
            if (ltype != null) {
                return ltype.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Converter>() {
                    @Override
                    public Optional<Converter> visit(
                            LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                        if (field.isRepetition(Type.Repetition.REPEATED)) {
                            return of(new RepeatedPrimitiveConverter(field.getName()));
                        }
                        return of(new MapConverter.StringConverter(field.getName()));
                    }

                    @Override
                    public Optional<Converter> visit(
                            LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                            return of(new DecimalConverter(field.getName(), decimalLogicalType.getScale()));
                    }
                }).orElse(new SimplePrimitiveConverter(field.getName()));
            }
            return new SimplePrimitiveConverter(field.getName());
        } else {
            throw new IllegalStateException("Unsupported format");
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    Map<String, Object> getCurrentRecord() {
        return record;
    }

    @Override
    public void start() {
        record = new HashMap<>();
    }

    @Override
    public void end() {
        if (parent != null) {
            parent.getCurrentRecord().put(name, record);
        }
    }

    private class SimplePrimitiveConverter extends PrimitiveConverter {

        protected final String name;

        public SimplePrimitiveConverter(String name) {
            this.name = name;
        }

        @Override
        public void addBinary(Binary value) {
            record.put(name, value.toStringUsingUTF8());
        }

        @Override
        public void addBoolean(boolean value) {
            record.put(name, value);
        }

        @Override
        public void addDouble(double value) {
            record.put(name, value);
        }

        @Override
        public void addFloat(float value) {
            record.put(name, value);
        }

        @Override
        public void addInt(int value) {
            record.put(name, value);
        }

        @Override
        public void addLong(long value) {
            record.put(name, value);
        }
    }

    private class StringConverter extends SimplePrimitiveConverter {

        public StringConverter(String name) {
            super(name);
        }

        @Override
        public void addBinary(Binary value) {
            record.put(name, value.toStringUsingUTF8());
        }
    }

    private class RepeatedPrimitiveConverter extends SimplePrimitiveConverter {

        public RepeatedPrimitiveConverter(String name) {
            super(name);
        }

        @Override
        public void addBinary(Binary value) {
            List array = (List) record.get(name);
            if (array == null) {
                array = new ArrayList<>();
                record.put(name, array);
            }
            array.add(value.toStringUsingUTF8());
        }
    }

    private class DecimalConverter extends PrimitiveConverter {
        private final String name;
        private final int scale;

        public DecimalConverter(String name, int scale) {
            this.name = name;
            this.scale = scale;
        }

        @Override
        public void addBinary(Binary value) {
            BigInteger unscaledValue = new BigInteger(value.getBytes());
            BigDecimal decimalValue = new BigDecimal(unscaledValue, scale);
            record.put(name, decimalValue);
        }

        @Override
        public void addLong(long value) {
            BigInteger unscaledValue = BigInteger.valueOf(value);
            BigDecimal decimalValue = new BigDecimal(unscaledValue, scale);
            record.put(name, decimalValue);
        }
    }
}