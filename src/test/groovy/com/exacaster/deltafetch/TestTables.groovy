package com.exacaster.deltafetch

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class TestTables {
    private static final String ACTIVE_DATA_FILE = "part-00000-ee79de49-56b9-40f7-8601-b9319c0a7076-c000.snappy.parquet"

    static Path withoutActiveDataFile() {
        def source = Paths.get(TestTables.getResource("/test_data").toURI())
        def table = Files.createTempDirectory("delta-fetch-test")
        Files.walk(source).each { file ->
            def target = table.resolve(source.relativize(file).toString())
            if (Files.isDirectory(file)) {
                Files.createDirectories(target)
            } else {
                Files.copy(file, target)
            }
        }
        Files.delete(table.resolve(ACTIVE_DATA_FILE))
        return table
    }
}
