package com.dynamolite;

import java.io.Serializable;

/**
 * Request represents a client request to the distributed system.
 */
public class Request implements Serializable {
    private static final long serialVersionUID = 2L;

    public enum Type {
        PUT,
        GET,
        DELETE,
        HEARTBEAT
    }

    private final Type type;
    private final String key;
    private final String value;
    private final VersionVector version;
    private final boolean replicaRead;

    public Request(Type type, String key, String value) {
        this(type, key, value, null, false);
    }

    public Request(Type type, String key, String value, VersionVector version) {
        this(type, key, value, version, false);
    }

    public Request(Type type, String key, String value, VersionVector version, boolean replicaRead) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.version = version;
        this.replicaRead = replicaRead;
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

    public VersionVector getVersion() {
        return version;
    }

    public boolean isReplicaRead() {
        return replicaRead;
    }
} 