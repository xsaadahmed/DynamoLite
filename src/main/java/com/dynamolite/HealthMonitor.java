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
 *
 * A node is marked failed if it has not responded to a heartbeat for longer
 * than FAILURE_THRESHOLD_MS. The clock starts on the first failed check for
 * that node — this ensures nodes that fail before their first successful
 * heartbeat are still eventually evicted.
 */
public class HealthMonitor {
    private static final Logger logger = LoggerFactory.getLogger(HealthMonitor.class);
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long FAILURE_THRESHOLD_MS = 3000;

    private final Node node;
    private final ConsistentHashRing hashRing;

    // Tracks the last successful heartbeat time for each peer node.
    private final Map<String, Long> lastHeartbeat;

    // Tracks when we first noticed a node was unreachable.
    // Null entry means the node has been responding normally.
    private final Map<String, Long> firstFailureTime;

    private volatile boolean running;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> healthCheckTask;

    public HealthMonitor(Node node, ConsistentHashRing hashRing) {
        this.node = node;
        this.hashRing = hashRing;
        this.lastHeartbeat = new ConcurrentHashMap<>();
        this.firstFailureTime = new ConcurrentHashMap<>();
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
                    recordFailure(nodeId);
                    continue;
                }
                connection.sendRequest(new Request(Request.Type.HEARTBEAT, null, null));
                // Heartbeat succeeded — update last seen and clear any failure record
                lastHeartbeat.put(nodeId, System.currentTimeMillis());
                firstFailureTime.remove(nodeId);
            } catch (IOException e) {
                recordFailure(nodeId);
            }
        }
    }

    /**
     * Records a failed heartbeat attempt for the given node.
     * On the first failure, we start the failure clock.
     * Once the clock exceeds FAILURE_THRESHOLD_MS, the node is evicted.
     */
    private void recordFailure(String nodeId) {
        long now = System.currentTimeMillis();
        // Start the failure clock if this is the first missed heartbeat
        firstFailureTime.putIfAbsent(nodeId, now);
        long failingSince = firstFailureTime.get(nodeId);
        if (now - failingSince > FAILURE_THRESHOLD_MS) {
            handleNodeFailure(nodeId);
        }
    }

    private void handleNodeFailure(String nodeId) {
        logger.error("Node {} detected as failed — removing from ring", nodeId);
        hashRing.removeNode(nodeId);
        lastHeartbeat.remove(nodeId);
        firstFailureTime.remove(nodeId);
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