package com.exacaster.deltafetch.rest.security;

import io.micronaut.http.HttpRequest;
import io.micronaut.security.authentication.AuthenticationProvider;
import io.micronaut.security.authentication.AuthenticationRequest;
import io.micronaut.security.authentication.AuthenticationResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class BasicAuthenticationProvider implements AuthenticationProvider {
    private final String username;
    private final String password;

    public BasicAuthenticationProvider(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Publisher<AuthenticationResponse> authenticate(HttpRequest<?> httpRequest,
                                                          AuthenticationRequest<?, ?> authenticationRequest) {
        final String providedUsername = (String) authenticationRequest.getIdentity();
        final String providedPassword = (String) authenticationRequest.getSecret();
        if (username.equals(providedUsername) && password.equals(providedPassword)) {
            return Mono.just(AuthenticationResponse.success(username));
        }
        return Mono.just(AuthenticationResponse.failure("Invalid username or password"));
    }
}
