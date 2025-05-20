package com.dynamolite;

import java.io.*;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeConnection handles communication between nodes in the distributed system.
 */
public class NodeConnection {
    private static final Logger logger = LoggerFactory.getLogger(NodeConnection.class);
    private final String nodeId;
    private final int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public NodeConnection(String nodeId, int port) throws IOException {
        this.nodeId = nodeId;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        try {
            socket = new Socket("localhost", port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            logger.error("Error connecting to node {}: {}", nodeId, e.getMessage());
            throw e;
        }
    }

    public Response sendRequest(Request request) throws IOException {
        try {
            out.writeObject(request);
            return (Response) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Error deserializing response", e);
        }
    }

    public void close() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            logger.error("Error closing connection to node {}: {}", nodeId, e.getMessage());
        }
    }
} 