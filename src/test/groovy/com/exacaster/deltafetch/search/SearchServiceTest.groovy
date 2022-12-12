package com.exacaster.deltafetch.search

import com.exacaster.deltafetch.search.delta.DeltaStatsReader
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
        def statsReader = new DeltaStatsReader(conf, cache)
        def svc = new SearchService(statsReader, new Configuration())
        def path = getClass().getResource("/test_data").toString()

        when:
        def result = svc.findOne(path, [new ColumnValueFilter("user_id", "912740210653_1451011")], true)

        then:
        result.isPresent()
        result.get().getResource().get("user_id") == "912740210653_1451011"

        when: "searching with non-existent value"
        result = svc.findOne(path, [new ColumnValueFilter("user_id", "none")], true)

        then: "returns empty"
        result.isEmpty()
    }
}
