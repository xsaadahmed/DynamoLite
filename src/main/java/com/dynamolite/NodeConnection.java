package com.dynamolite;

import java.io.*;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeConnection handles communication between nodes in the distributed system.
 * Automatically reconnects if a previously established connection goes stale
 * (e.g. the remote node restarted).
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
            // ObjectOutputStream MUST be created and flushed before ObjectInputStream
            // to avoid deadlock when both sides open streams simultaneously.
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(socket.getInputStream());
        } catch (IOException e) {
            logger.error("Error connecting to node {}: {}", nodeId, e.getMessage());
            throw e;
        }
    }

    /**
     * Sends a request and returns the response.
     * If the connection has gone stale (e.g. remote node restarted), attempts
     * one reconnect before propagating the failure.
     */
    public Response sendRequest(Request request) throws IOException {
        try {
            out.writeObject(request);
            out.flush();
            return (Response) in.readObject();
        } catch (IOException e) {
            // Connection may be stale — try reconnecting once
            logger.warn("Connection to node {} lost ({}), attempting reconnect...", nodeId, e.getMessage());
            try {
                close();
                connect();
                out.writeObject(request);
                out.flush();
                return (Response) in.readObject();
            } catch (IOException | ClassNotFoundException reconnectEx) {
                throw new IOException("Failed to send request to node " + nodeId +
                        " after reconnect attempt: " + reconnectEx.getMessage(), reconnectEx);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Error deserializing response from node " + nodeId, e);
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