package com.dynamolite;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Objects;

/**
 * VersionVector implements a vector clock for tracking data versions across nodes.
 * It helps in detecting concurrent modifications and resolving conflicts.
 */
public class VersionVector {
    private final Map<String, Long> vector;
    private final String nodeId;

    public VersionVector(String nodeId) {
        this.nodeId = nodeId;
        this.vector = new ConcurrentHashMap<>();
        this.vector.put(nodeId, 0L);
    }

    /**
     * Increments the version for the current node
     */
    public void increment() {
        vector.compute(nodeId, (key, value) -> value == null ? 1L : value + 1);
    }

    /**
     * Updates the version vector with another vector's values
     */
    public void update(VersionVector other) {
        other.vector.forEach((node, version) -> 
            vector.merge(node, version, Math::max)
        );
    }

    /**
     * Compares this version vector with another
     * @return 1 if this vector is newer, -1 if other is newer, 0 if concurrent
     */
    public int compare(VersionVector other) {
        boolean thisGreater = false;
        boolean otherGreater = false;

        for (String node : vector.keySet()) {
            long thisVersion = vector.getOrDefault(node, 0L);
            long otherVersion = other.vector.getOrDefault(node, 0L);

            if (thisVersion > otherVersion) {
                thisGreater = true;
            } else if (otherVersion > thisVersion) {
                otherGreater = true;
            }
        }

        if (thisGreater && !otherGreater) return 1;
        if (otherGreater && !thisGreater) return -1;
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionVector that = (VersionVector) o;
        return Objects.equals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vector);
    }

    @Override
    public String toString() {
        return vector.toString();
    }
} 