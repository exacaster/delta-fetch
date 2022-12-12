package com.exacaster.deltafetch.search;

public class SearchResult<T> {
    private final T resource;

    public SearchResult(T resource) {
        this.resource = resource;
    }

    public T getResource() {
        return resource;
    }
}
