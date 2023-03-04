package com.exacaster.deltafetch.rest;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.uri.UriMatchInfo;
import org.apache.commons.text.StringSubstitutor;

public final class RequestUtils {
    private RequestUtils() {
    }

    public static boolean isRequestForLatestData(HttpRequest request) {
        return request.getParameters().get("exact", Boolean.class).filter(val -> val).isPresent();
    }

    public static String buildDeltaPath(String deltaPtahTemplate, UriMatchInfo info) {
        return StringSubstitutor.replace(deltaPtahTemplate, info.getVariableValues(), "{", "}");
    }
}
