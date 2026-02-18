package com.dynamolite;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors node health via heartbeats and removes failed nodes from the ring.
 */
public class HealthMonitor {
    private static final Logger logger = LoggerFactory.getLogger(HealthMonitor.class);
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long FAILURE_THRESHOLD_MS = 3000;

    private final Node node;
    private final ConsistentHashRing hashRing;
    private final Map<String, Long> lastHeartbeat;
    private volatile boolean running;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> healthCheckTask;

    public HealthMonitor(Node node, ConsistentHashRing hashRing) {
        this.node = node;
        this.hashRing = hashRing;
        this.lastHeartbeat = new ConcurrentHashMap<>();
        this.running = true;
    }

    public void startHealthCheck() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        healthCheckTask = scheduler.scheduleAtFixedRate(
            this::checkHealth,
            HEARTBEAT_INTERVAL_MS,
            HEARTBEAT_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }

    private void checkHealth() {
        if (!running) {
            return;
        }
        String currentNodeId = node.getNodeId();
        Set<String> nodes = hashRing.getAllNodes();
        for (String nodeId : nodes) {
            if (nodeId.equals(currentNodeId)) {
                continue;
            }
            try {
                NodeConnection connection = node.getConnection(nodeId);
                if (connection == null) {
                    recordFailureIfThresholdExceeded(nodeId);
                    continue;
                }
                connection.sendRequest(new Request(Request.Type.HEARTBEAT, null, null));
                lastHeartbeat.put(nodeId, System.currentTimeMillis());
            } catch (IOException e) {
                recordFailureIfThresholdExceeded(nodeId);
            }
        }
    }

    private void recordFailureIfThresholdExceeded(String nodeId) {
        Long lastSeen = lastHeartbeat.get(nodeId);
        if (lastSeen == null) {
            return; // never seen a successful heartbeat; give node time to respond
        }
        if (System.currentTimeMillis() - lastSeen > FAILURE_THRESHOLD_MS) {
            handleNodeFailure(nodeId);
        }
    }

    private void handleNodeFailure(String nodeId) {
        logger.error("Node {} detected as failed", nodeId);
        hashRing.removeNode(nodeId);
        lastHeartbeat.remove(nodeId);
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void stop() {
        running = false;
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while shutting down health monitor");
            }
        }
    }
}
