package com.exacaster.deltafetch.search.delta

import com.exacaster.deltafetch.configuration.ResourceConfiguration
import com.exacaster.deltafetch.configuration.ResponseType
import spock.lang.Specification

class MetaCacheUpdateExecutorTest extends Specification {

    def "fetches meta only for static and cached resources"() {
        given:
        def metaReader = Mock(DeltaMetaReader) {
            findCachedMeta() >> ["s3a://example/1/bar": null]
        }
        def resourceConfig = new ResourceConfiguration([:], [
                new ResourceConfiguration.Resource("/foo", "s3a://example/foo", null, [], ResponseType.SINGLE, null),
                new ResourceConfiguration.Resource("/{id}/foo", "s3a://example/{id}/foo", null, [], ResponseType.SINGLE, null)
        ])
        def executor = new MetaCacheUpdateExecutor(metaReader, resourceConfig)

        when:
        executor.warmupResources()

        then:
        1 * metaReader.findMeta("s3a://example/foo", true)
        1 * metaReader.findMeta("s3a://example/1/bar", true)
    }
}
