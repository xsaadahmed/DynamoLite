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
    private final String nodeId;
    private final int port;
    private final Storage storage;
    private final ConsistentHashRing hashRing;
    private final Map<String, NodeConnection> connections;
    private final ExecutorService executorService;
    private final int readQuorum;
    private final int writeQuorum;
    private ServerSocket serverSocket;
    private volatile boolean running;

    public Node(int port, String dataDir, int readQuorum, int writeQuorum) {
        this.nodeId = UUID.randomUUID().toString();
        this.port = port;
        this.storage = new Storage(dataDir);
        this.hashRing = new ConsistentHashRing();
        this.connections = new ConcurrentHashMap<>();
        this.executorService = Executors.newCachedThreadPool();
        this.readQuorum = readQuorum;
        this.writeQuorum = writeQuorum;
        this.hashRing.addNode(nodeId);
    }

    public void start() {
        running = true;
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Node {} started on port {}", nodeId, port);

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

        Storage.Value value = storage.get(key);
        if (value == null) {
            return new Response(Response.Status.NOT_FOUND, "Key not found");
        }

        return new Response(Response.Status.SUCCESS, value.getData());
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
            connection.sendRequest(new Request(Request.Type.PUT, key, value));
        } catch (IOException e) {
            logger.error("Error replicating PUT to node {}: {}", nodeId, e.getMessage());
        }
    }

    private void replicateDelete(String nodeId, String key) {
        try {
            NodeConnection connection = getConnection(nodeId);
            connection.sendRequest(new Request(Request.Type.DELETE, key, null));
        } catch (IOException e) {
            logger.error("Error replicating DELETE to node {}: {}", nodeId, e.getMessage());
        }
    }

    private NodeConnection getConnection(String nodeId) throws IOException {
        return connections.computeIfAbsent(nodeId, id -> {
            try {
                return new NodeConnection(id, port);
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