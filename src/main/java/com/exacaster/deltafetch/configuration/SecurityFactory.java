package com.exacaster.deltafetch.configuration;

import com.exacaster.deltafetch.rest.security.BasicAuthenticationProvider;
import com.exacaster.deltafetch.rest.security.ClaimBasedCheck;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.security.token.RolesFinder;

import java.util.Collection;

@Factory
public class SecurityFactory {

    @Bean
    @Requires(property = "app.security.oauth2.allowed-scopes")
    public ClaimBasedCheck claimCheck(@Value("${app.security.oauth2.allowed-scopes}") Collection<String> claimTemplates,
                                      ResourceConfiguration configuration, RolesFinder rolesFinder) {
        return new ClaimBasedCheck(claimTemplates, configuration.getResources(), rolesFinder);
    }

    @Bean
    @Requires(property = "app.security.basic.enabled", value = "true")
    public BasicAuthenticationProvider basicAuthenticationProvider(
            @Value("${app.security.basic.username}") String username,
            @Value("${app.security.basic.password}") String password) {
        return new BasicAuthenticationProvider(username, password);
    }
}
