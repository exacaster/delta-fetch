package com.exacaster.deltafetch.rest;

public class APIResponse<T> {

    private final Long version;
    private final T data;

    public APIResponse(Long version, T resource) {
        this.version = version;
        this.data = resource;
    }

    public Long getVersion() {
        return version;
    }

    public T getData() {
        return data;
    }
}
