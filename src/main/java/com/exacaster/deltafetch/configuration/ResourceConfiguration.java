package com.exacaster.deltafetch.configuration;

import io.micronaut.context.annotation.ConfigurationInject;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.convert.format.MapFormat;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static io.micronaut.core.convert.format.MapFormat.MapTransformation.FLAT;
import static io.micronaut.core.naming.conventions.StringConvention.RAW;

@Introspected
@ConfigurationProperties("app")
public class ResourceConfiguration {
    private final Map<String, String> hadoopProps;
    private final List<Resource> resources;

    @ConfigurationInject
    public ResourceConfiguration(@MapFormat(transformation = FLAT, keyFormat = RAW) Map<String, String> hadoopProps, List<Resource> resources) {
        this.hadoopProps = hadoopProps;
        this.resources = resources;
    }

    public Map<String, String> getHadoopProps() {
        return hadoopProps;
    }

    public List<Resource> getResources() {
        return resources;
    }

    @Introspected
    public static class Resource {

        private final String path;

        private final String deltaPath;

        private final String schemaPath;

        private final List<FilterVariable> filterVariables;

        public Resource(String path, String deltaPath, String schemaPath, List<FilterVariable> filterVariables) {
            this.path = path;
            this.deltaPath = deltaPath;
            this.schemaPath = schemaPath;
            this.filterVariables = filterVariables;
        }

        public String getPath() {
            return this.path;
        }

        public String getDeltaPath() {
            return deltaPath;
        }

        public @Nullable String getSchemaPath() {
            return schemaPath;
        }

        public List<FilterVariable> getFilterVariables() {
            return filterVariables;
        }
    }

    @Introspected
    public static class FilterVariable {
        private final String column;
        private final String pathColumn;
        private final String staticValue;
        private final String pathVariable;

        public FilterVariable(String column, String pathColumn, String staticValue, String pathVariable) {
            this.column = column;
            this.pathColumn = pathColumn;
            this.staticValue = staticValue;
            this.pathVariable = pathVariable;
        }

        public String getColumn() {
            return column;
        }

        public String getPathColumn() {
            return pathColumn;
        }

        public String getStaticValue() {
            return staticValue;
        }

        public String getPathVariable() {
            return pathVariable;
        }
    }
}
