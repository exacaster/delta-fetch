package com.exacaster.deltafetch.rest.security;

import com.exacaster.deltafetch.configuration.ResourceConfiguration;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.uri.UriMatchTemplate;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.rules.AbstractSecurityRule;
import io.micronaut.security.rules.SecurityRuleResult;
import io.micronaut.security.token.RolesFinder;
import io.micronaut.web.router.RouteMatch;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ClaimBasedCheck extends AbstractSecurityRule {
    private final Collection<String> allowedClaims;
    private final List<UriMatchTemplate> matchers;

    public ClaimBasedCheck(
            Collection<String> allowedClaims,
            Collection<ResourceConfiguration.Resource> resources,
            RolesFinder rolesFinder) {
        super(rolesFinder);
        this.allowedClaims = allowedClaims;
        this.matchers = resources.stream()
                .flatMap(resource -> {
                    List<UriMatchTemplate> list = new ArrayList<>();
                    list.add(new UriMatchTemplate(resource.getPath()));
                    if (resource.getSchemaPath() != null) {
                        list.add(new UriMatchTemplate(resource.getSchemaPath()));
                    }
                    return list.stream();
                }).collect(Collectors.toList());
    }

    @Override
    public Publisher<SecurityRuleResult> check(HttpRequest<?> request, RouteMatch<?> routeMatch,
                                               Authentication authentication) {
        var scopes = authentication != null ? (List<String>) authentication.getAttributes().get("scope") : null;
        if (scopes == null || scopes.isEmpty()) {
            return Mono.just(SecurityRuleResult.REJECTED);
        }
        for (var matcher : this.matchers) {
            var allowed = matcher.match(request.getUri())
                    .stream()
                    .flatMap(info -> allowedClaims.stream()
                            .map(claim -> StrSubstitutor.replace(claim, info.getVariableValues(), "{", "}")))
                    .anyMatch(scopes::contains);
            if (allowed) {
                return Mono.just(SecurityRuleResult.ALLOWED);
            }
        }

        return Mono.just(SecurityRuleResult.REJECTED);
    }
}
