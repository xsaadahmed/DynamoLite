package com.dynamolite;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ConsistentHashRing implements consistent hashing for distributing keys across nodes.
 * Uses MD5-based hashing with virtual nodes for better load distribution.
 */
public class ConsistentHashRing {
    private final ConcurrentSkipListMap<Long, String> ring;
    private final Map<String, Set<Long>> nodeToVirtualNodes;
    private final int virtualNodesPerNode;
    private static final int DEFAULT_VIRTUAL_NODES = 3;

    public ConsistentHashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashRing(int virtualNodesPerNode) {
        this.ring = new ConcurrentSkipListMap<>();
        this.nodeToVirtualNodes = new ConcurrentHashMap<>();
        this.virtualNodesPerNode = virtualNodesPerNode;
    }

    /**
     * Adds a node to the ring with virtual nodes.
     */
    public void addNode(String nodeId) {
        Set<Long> virtualNodes = new HashSet<>();
        for (int i = 0; i < virtualNodesPerNode; i++) {
            String virtualNodeId = nodeId + "#" + i;
            long hash = hash(virtualNodeId);
            ring.put(hash, nodeId);
            virtualNodes.add(hash);
        }
        nodeToVirtualNodes.put(nodeId, virtualNodes);
    }

    /**
     * Removes a node and its virtual nodes from the ring.
     */
    public void removeNode(String nodeId) {
        Set<Long> virtualNodes = nodeToVirtualNodes.remove(nodeId);
        if (virtualNodes != null) {
            virtualNodes.forEach(ring::remove);
        }
    }

    /**
     * Gets the node responsible for a given key.
     */
    public String getNode(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No nodes in the ring");
        }
        long hash = hash(key);
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);
        return entry != null ? entry.getValue() : ring.firstEntry().getValue();
    }

    /**
     * Gets the N nodes responsible for a given key (preference list).
     */
    public List<String> getNodes(String key, int n) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No nodes in the ring");
        }
        if (n > ring.size()) {
            throw new IllegalArgumentException("Requested more nodes than available");
        }

        long hash = hash(key);
        Set<String> nodes = new LinkedHashSet<>();
        NavigableMap<Long, String> tailMap = ring.tailMap(hash, true);

        // Add nodes from the tail of the ring
        tailMap.values().forEach(nodes::add);

        // Wrap around to the beginning if needed
        if (nodes.size() < n) {
            ring.headMap(hash, false).values().forEach(nodes::add);
        }

        return nodes.stream().limit(n).collect(Collectors.toList());
    }

    /**
     * MD5-based hash for uniform key distribution across the ring.
     * Falls back to hashCode() if MD5 is unavailable (should never happen on JVM).
     */
    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(key.getBytes());
            // Combine first 8 bytes into a long for a wide hash space
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash = (hash << 8) | (bytes[i] & 0xFF);
            }
            return Math.abs(hash);
        } catch (NoSuchAlgorithmException e) {
            // MD5 is guaranteed by the JVM spec — this path should never be reached
            return Math.abs(key.hashCode());
        }
    }

    /**
     * Returns the number of physical nodes in the ring.
     */
    public int size() {
        return nodeToVirtualNodes.size();
    }

    /**
     * Returns all physical nodes in the ring.
     */
    public Set<String> getAllNodes() {
        return new HashSet<>(nodeToVirtualNodes.keySet());
    }
}