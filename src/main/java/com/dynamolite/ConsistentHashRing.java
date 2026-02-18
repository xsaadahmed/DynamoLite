package com.dynamolite;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * ConsistentHashRing implements consistent hashing for distributing keys across nodes.
 * It uses virtual nodes to ensure better load distribution.
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
     * Adds a node to the ring with virtual nodes
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
     * Removes a node and its virtual nodes from the ring
     */
    public void removeNode(String nodeId) {
        Set<Long> virtualNodes = nodeToVirtualNodes.remove(nodeId);
        if (virtualNodes != null) {
            virtualNodes.forEach(ring::remove);
        }
    }

    /**
     * Gets the node responsible for a given key
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
     * Gets the N nodes responsible for a given key
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
        
        // Add nodes from the tail
        tailMap.values().forEach(nodes::add);
        
        // If we need more nodes, wrap around to the beginning
        if (nodes.size() < n) {
            ring.headMap(hash, false).values().forEach(nodes::add);
        }

        return nodes.stream().limit(n).collect(Collectors.toList());
    }

    /**
     * Simple hash function for the ring
     */
    private long hash(String key) {
        return Math.abs(key.hashCode());
    }

    /**
     * Returns the number of nodes in the ring
     */
    public int size() {
        return nodeToVirtualNodes.size();
    }

    /**
     * Returns all nodes in the ring
     */
    public Set<String> getAllNodes() {
        return new HashSet<>(nodeToVirtualNodes.keySet());
    }
} 