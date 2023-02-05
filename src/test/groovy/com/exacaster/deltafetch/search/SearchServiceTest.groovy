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
}
