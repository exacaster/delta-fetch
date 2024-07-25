package com.exacaster.deltafetch.search

import com.exacaster.deltafetch.search.delta.DeltaMetaReader
import io.micronaut.cache.SyncCache
import org.apache.hadoop.conf.Configuration
import spock.lang.Specification

class SearchServiceTest extends Specification {

    def works() {
        given:
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def svc = new SearchService(statsReader, new Configuration())
        def path = getClass().getResource("/test_data").toString()

        when:
        def result = svc.find(path, [new ColumnValueFilter("user_id", "912740210653_1451011")], true, 1).findFirst()

        then:
        result.isPresent()
        result.get().getValue().get("user_id") == "912740210653_1451011"

        when: "searching with non-existent value"
        result = svc.find(path, [new ColumnValueFilter("user_id", "none")], true, 1).findFirst()

        then: "returns empty"
        result.isEmpty()
    }
    def worksWithDifferentTypes() {
        given:
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def svc = new SearchService(statsReader, new Configuration())
        def path = getClass().getResource("/test_data_types").toString()

        when:
        def result = svc.find(path, [new ColumnValueFilter("user_id", "555")], true, 1).findFirst()

        then:
        result.isPresent()
        result.get().getValue().get("trait_string") == "ACTIVE"
        result.get().getValue().get("trait_decimal18_3") == 5.555
        result.get().getValue().get("trait_decimal21_3") == 55.123
        result.get().getValue().get("trait_double") == 12345.678
        result.get().getValue().get("trait_float") == 12345.678F
        result.get().getValue().get("trait_int") == 12345
        result.get().getValue().get("trait_bigint") == 123456789012345
        result.get().getValue().get("trait_boolean") == true
    }
}
