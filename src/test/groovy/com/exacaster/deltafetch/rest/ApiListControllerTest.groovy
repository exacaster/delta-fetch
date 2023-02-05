package com.exacaster.deltafetch.rest

import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import jakarta.inject.Inject
import spock.lang.Specification

@MicronautTest(environments = ["it"])
class ApiListControllerTest extends Specification implements TestPropertyProvider {
    @Inject
    @Client("/")
    HttpClient client

    def works() {
        when:
        def response = client.toBlocking().retrieve(HttpRequest.GET("/api/users/PREPAID?limit=2"), Map.class)

        then:
        response.get("data").size() == 2
        response.get("data").get(0).get("user_id") == "912740210653_1389"
        response.get("data").get(1).get("user_id") == "912740210653_138910"

        when: "no limit parameter"
        response = client.toBlocking().retrieve(HttpRequest.GET("/api/users/PREPAID"), Map.class)

        then: "use max allowed"
        response.get("data").size() == 10

        when: "limit more than max allowed"
        response = client.toBlocking().retrieve(HttpRequest.GET("/api/users/PREPAID?limit=20"), Map.class)

        then: "use max allowed"
        response.get("data").size() == 10

        when: "does not exist"
        client.toBlocking().exchange(HttpRequest.GET("/api/users/none")).status()

        then: "not found"
        HttpClientResponseException ex = thrown(HttpClientResponseException)
        ex.status.code == 404
    }

    @Override
    Map<String, String> getProperties() {
        return [
                app: [
                        resources: [
                                [
                                        path              : '/api/users/{sub_type}',
                                        'response-type'   : 'LIST',
                                        'max-results'     : 10,
                                        'delta-path'      : getClass().getResource("/test_data").toString(),
                                        'filter-variables': [[column: 'sub_type', 'path-variable': 'sub_type']],
                                ]
                        ]
                ]
        ]
    }
}
