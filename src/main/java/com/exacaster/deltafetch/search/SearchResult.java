package com.exacaster.deltafetch.search;

public class SearchResult<T> {
    private final T data;

    public SearchResult(T resource) {
        this.data = resource;
    }

    public T getData() {
        return data;
    }
}
