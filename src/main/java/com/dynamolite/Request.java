package com.dynamolite;

import java.io.Serializable;

/**
 * Request represents a client request to the distributed system.
 */
public class Request implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Type {
        PUT,
        GET,
        DELETE
    }

    private final Type type;
    private final String key;
    private final String value;

    public Request(Type type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
} 