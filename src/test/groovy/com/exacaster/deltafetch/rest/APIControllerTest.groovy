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
class APIControllerTest extends Specification implements TestPropertyProvider {
    @Inject
    @Client("/")
    HttpClient client

    def works() {
        when:
        def response = client.toBlocking().retrieve(HttpRequest.GET("/api/users/912740210653_1451011"), Map.class)

        then:
        response.get("data").get("user_id") == "912740210653_1451011"
        response.get("data").get("sub_type") == "PREPAID"

        when: "does not exist"
        client.toBlocking().exchange(HttpRequest.GET("/api/users/none")).status()

        then: "not found"
        HttpClientResponseException ex = thrown(HttpClientResponseException)
        ex.status.code == 404

        when: "getting schema"
        response = client.toBlocking().retrieve(HttpRequest.GET("/api/schemas/users"), Map.class)
        def fields = response.get("data").get("fields")

        then: "returns schema"
        fields.size() == 5
        fields.get(0).get("name") == "user_id"
        fields.get(0).get("type") == "string"
        fields.get(0).get("nullable") == true
    }

    @Override
    Map<String, String> getProperties() {
        return [
                app: [
                        resources: [
                                [
                                        path              : '/api/users/{user_id}',
                                        'delta-path'      : getClass().getResource("/test_data").toString(),
                                        'filter-variables': [[column: 'user_id', 'path-variable': 'user_id']],
                                        'schema-path'     : '/api/schemas/users'
                                ]
                        ]
                ]
        ]
    }
}
