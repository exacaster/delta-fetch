package com.exacaster.deltafetch.search

import com.exacaster.deltafetch.search.delta.DeltaMetaReader
import io.micronaut.cache.SyncCache
import org.apache.hadoop.conf.Configuration
import spock.lang.Specification
import java.util.concurrent.Executors

class SearchServiceTest extends Specification {

    def works() {
        given:
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def executorService = Executors.newFixedThreadPool(2)
        def svc = new SearchService(statsReader, new Configuration(), executorService)
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

        cleanup:
        executorService.shutdown()
    }
    def worksWithDifferentTypes() {
        given:
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def executorService = Executors.newFixedThreadPool(2)
        def svc = new SearchService(statsReader, new Configuration(), executorService)
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

        cleanup:
        executorService.shutdown()
    }

    def "should respect limit and not exceed queue capacity"() {
        given: "a search service with test data (15 records available)"
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def executorService = Executors.newFixedThreadPool(4)
        def svc = new SearchService(statsReader, new Configuration(), executorService)
        def path = getClass().getResource("/test_data").toString()

        when: "requesting a small limit"
        def results = svc.find(path, [], true, 5).toList()

        then: "returns exactly the limit (not more, not less)"
        results.size() == 5
        and: "all results are valid"
        results.every { it.getValue() != null }

        cleanup:
        executorService.shutdown()
    }

    def "should handle concurrent file reads without race conditions"() {
        given: "a search service with multiple parallel workers (15 records available)"
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def executorService = Executors.newFixedThreadPool(10)
        def svc = new SearchService(statsReader, new Configuration(), executorService)
        def path = getClass().getResource("/test_data").toString()

        when: "reading with high parallelism and limit near total"
        def results = svc.find(path, [], true, 12).toList()

        then: "returns exactly the limit, proving no race condition caused over-collection"
        results.size() == 12
        and: "all results are valid with no duplicates"
        results.every { it.getValue() != null }
        def userIds = results.collect { it.getValue().get("user_id") }
        userIds.unique().size() == userIds.size()

        cleanup:
        executorService.shutdown()
    }

    def "should handle empty results gracefully"() {
        given: "a search service"
        def cache = Mock(SyncCache) {
            get(*_) >> Optional.empty()
        }
        def conf = new Configuration()
        def statsReader = new DeltaMetaReader(conf, cache)
        def executorService = Executors.newFixedThreadPool(2)
        def svc = new SearchService(statsReader, new Configuration(), executorService)
        def path = getClass().getResource("/test_data").toString()

        when: "searching with filters that match nothing"
        def results = svc.find(path, [new ColumnValueFilter("user_id", "nonexistent_user_123456789")], true, 10).toList()

        then: "returns empty list"
        results.isEmpty()

        cleanup:
        executorService.shutdown()
    }
}
