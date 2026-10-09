package com.exacaster.deltafetch.rest

import com.exacaster.deltafetch.TestTables
import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

@MicronautTest(environments = ["it"])
class ApiIncompleteSearchTest extends Specification implements TestPropertyProvider {
    @Inject
    @Client("/")
    HttpClient client

    def "responds 503 instead of 404 when a data file cannot be read"() {
        when:
        client.toBlocking().exchange(HttpRequest.GET("/api/users/912740210653_1451011"))

        then:
        HttpClientResponseException ex = thrown(HttpClientResponseException)
        ex.status.code == 503
    }

    @Override
    Map<String, String> getProperties() {
        return [
                app: [
                        resources: [
                                [
                                        path              : '/api/users/{user_id}',
                                        'delta-path'      : TestTables.withoutActiveDataFile().toUri().toString(),
                                        'filter-variables': [[column: 'user_id', 'path-variable': 'user_id']],
                                        'schema-path'     : '/api/schemas/users'
                                ]
                        ]
                ]
        ]
    }
}
