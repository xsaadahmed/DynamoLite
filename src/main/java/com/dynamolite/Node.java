package com.dynamolite;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node represents a single server in the distributed system.
 * It handles client requests and coordinates with other nodes.
 */
public class Node {
    private static final Logger logger = LoggerFactory.getLogger(Node.class);
    private static boolean HEALTH_CHECK_ENABLED = true;
    private final String nodeId;
    private final int port;
    private final Storage storage;
    private final ConsistentHashRing hashRing;
    private final Map<String, NodeConnection> connections;
    private final ExecutorService executorService;
    private final int readQuorum;
    private final int writeQuorum;
    private final HealthMonitor healthMonitor;
    private final Map<String, Integer> peerPorts;
    private ServerSocket serverSocket;
    private volatile boolean running;

    public Node(int port, String dataDir, int readQuorum, int writeQuorum) {
        this.nodeId = UUID.randomUUID().toString();
        this.port = port;
        this.storage = new Storage(dataDir);
        this.hashRing = new ConsistentHashRing();
        this.healthMonitor = new HealthMonitor(this, hashRing);
        this.connections = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
        this.readQuorum = readQuorum;
        this.writeQuorum = writeQuorum;
        this.peerPorts = new ConcurrentHashMap<>();
        this.hashRing.addNode(nodeId);
    }

    /**
     * Adds a peer node to the ring and stores its port for connections.
     */
    public void addPeer(String nodeId, int port) {
        hashRing.addNode(nodeId);
        peerPorts.put(nodeId, port);
    }

    public static void setHealthCheckEnabled(boolean enabled) {
        HEALTH_CHECK_ENABLED = enabled;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void start() {
        running = true;
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Node {} started on port {}", nodeId, port);
            if (HEALTH_CHECK_ENABLED) {
                healthMonitor.startHealthCheck();
            }

            while (running) {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            if (running) {
                logger.error("Error in node server: {}", e.getMessage());
            }
        }
    }

    public void stop() {
        running = false;
        healthMonitor.setRunning(false);
        healthMonitor.stop();
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (IOException | InterruptedException e) {
            logger.error("Error stopping node: {}", e.getMessage());
        }
    }

    private void handleClient(Socket clientSocket) {
        try (ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream())) {

            Request request = (Request) in.readObject();
            Response response = processRequest(request);
            out.writeObject(response);
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error handling client request: {}", e.getMessage());
        }
    }

    private Response processRequest(Request request) {
        switch (request.getType()) {
            case PUT:
                return handlePut(request);
            case GET:
                return handleGet(request);
            case DELETE:
                return handleDelete(request);
            case HEARTBEAT:
                return new Response(Response.Status.SUCCESS, "PONG");
            default:
                return new Response(Response.Status.ERROR, "Unknown request type");
        }
    }

    private Response handlePut(Request request) {
        String key = request.getKey();
        List<String> nodes = hashRing.getNodes(key, writeQuorum);
        
        if (!nodes.contains(nodeId)) {
            return new Response(Response.Status.ERROR, "Not responsible for this key");
        }

        VersionVector version = new VersionVector(nodeId);
        version.increment();
        storage.put(key, request.getValue(), version);

        // Replicate to other nodes
        for (String node : nodes) {
            if (!node.equals(nodeId)) {
                replicatePut(node, key, request.getValue(), version);
            }
        }

        return new Response(Response.Status.SUCCESS, "Value stored");
    }

    private Response handleGet(Request request) {
        String key = request.getKey();
        List<String> nodes = hashRing.getNodes(key, readQuorum);

        if (!nodes.contains(nodeId)) {
            return new Response(Response.Status.ERROR, "Not responsible for this key");
        }

        // Replica read: another node is doing quorum read; return only local value
        if (request.isReplicaRead()) {
            Storage.Value local = storage.get(key);
            if (local == null) {
                return new Response(Response.Status.NOT_FOUND, "Key not found");
            }
            return new Response(Response.Status.SUCCESS, local.getData());
        }

        // Quorum read: collect responses from all nodes in the read quorum
        List<Storage.Value> valuesWithVersion = new ArrayList<>();
        for (String node : nodes) {
            if (node.equals(nodeId)) {
                Storage.Value local = storage.get(key);
                if (local != null) {
                    valuesWithVersion.add(local);
                }
            } else {
                try {
                    NodeConnection connection = getConnection(node);
                    if (connection != null) {
                        Response r = connection.sendRequest(new Request(Request.Type.GET, key, null, null, true));
                        if (r.isSuccess() && r.getMessage() != null) {
                            valuesWithVersion.add(new Storage.Value(r.getMessage(), null));
                        }
                    }
                } catch (IOException e) {
                    logger.debug("Quorum read: failed to get from node {}: {}", node, e.getMessage());
                }
            }
        }

        if (valuesWithVersion.isEmpty()) {
            return new Response(Response.Status.NOT_FOUND, "Key not found");
        }

        // Pick latest by version (prefer entries with non-null version; then compare)
        Storage.Value best = valuesWithVersion.get(0);
        for (int i = 1; i < valuesWithVersion.size(); i++) {
            Storage.Value candidate = valuesWithVersion.get(i);
            if (candidate.getVersion() != null && best.getVersion() != null) {
                if (candidate.getVersion().compare(best.getVersion()) > 0) {
                    best = candidate;
                }
            } else if (candidate.getVersion() != null) {
                best = candidate;
            }
        }
        return new Response(Response.Status.SUCCESS, best.getData());
    }

    private Response handleDelete(Request request) {
        String key = request.getKey();
        List<String> nodes = hashRing.getNodes(key, writeQuorum);
        
        if (!nodes.contains(nodeId)) {
            return new Response(Response.Status.ERROR, "Not responsible for this key");
        }

        storage.remove(key);

        // Replicate delete to other nodes
        for (String node : nodes) {
            if (!node.equals(nodeId)) {
                replicateDelete(node, key);
            }
        }

        return new Response(Response.Status.SUCCESS, "Value deleted");
    }

    private void replicatePut(String nodeId, String key, String value, VersionVector version) {
        try {
            NodeConnection connection = getConnection(nodeId);
            if (connection != null) {
                connection.sendRequest(new Request(Request.Type.PUT, key, value, version));
            }
        } catch (IOException e) {
            logger.error("Error replicating PUT to node {}: {}", nodeId, e.getMessage());
        }
    }

    private void replicateDelete(String nodeId, String key) {
        try {
            NodeConnection connection = getConnection(nodeId);
            if (connection != null) {
                connection.sendRequest(new Request(Request.Type.DELETE, key, null, null));
            }
        } catch (IOException e) {
            logger.error("Error replicating DELETE to node {}: {}", nodeId, e.getMessage());
        }
    }

    /**
     * Returns the connection to the given node, or null if the connection could not be established.
     */
    public NodeConnection getConnection(String nodeId) {
        return connections.computeIfAbsent(nodeId, id -> {
            try {
                int targetPort = peerPorts.getOrDefault(id, port);
                return new NodeConnection(id, targetPort);
            } catch (IOException e) {
                logger.error("Error creating connection to node {}: {}", id, e.getMessage());
                return null;
            }
        });
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java Node <port>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String dataDir = "data/node_" + port;
        Node node = new Node(port, dataDir, 2, 2); // Example quorum sizes
        node.start();
    }
} 