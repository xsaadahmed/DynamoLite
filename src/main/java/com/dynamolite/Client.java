package com.dynamolite;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client provides a command-line interface to interact with the distributed system.
 */
public class Client implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private final String host;
    private final int port;
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        try {
            socket = new Socket(host, port);
            socket.setSoTimeout(15000);
            out = new ObjectOutputStream(socket.getOutputStream());
            out.flush();
            in = new ObjectInputStream(socket.getInputStream());
            logger.info("Connected to server at {}:{}", host, port);
        } catch (IOException e) {
            logger.error("Error connecting to server: {}", e.getMessage());
            throw e;
        }
    }

    public Response sendRequest(Request request) throws IOException {
        try {
            out.writeObject(request);
            out.flush();
            return (Response) in.readObject();
        } catch (SocketTimeoutException e) {
            logger.error("Request timeout after 15 seconds");
            throw new IOException("Request timeout after 15 seconds", e);
        } catch (ClassNotFoundException e) {
            throw new IOException("Error deserializing response", e);
        }
    }

    @Override
    public void close() {
        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (socket != null) socket.close();
        } catch (IOException e) {
            logger.error("Error closing connection: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Client <host> <port>");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        try (Client client = new Client(host, port);
             Scanner scanner = new Scanner(System.in)) {

            client.connect();

            while (true) {
                System.out.print("Enter command (PUT/GET/DELETE/QUIT): ");
                String command = scanner.nextLine().toUpperCase();

                if (command.equals("QUIT")) {
                    break;
                }

                try {
                    switch (command) {
                        case "PUT":
                            System.out.print("Enter key: ");
                            String putKey = scanner.nextLine();
                            System.out.print("Enter value: ");
                            String value = scanner.nextLine();
                            Response putResponse = client.sendRequest(new Request(Request.Type.PUT, putKey, value));
                            System.out.println("Response: " + putResponse.getMessage());
                            break;

                        case "GET":
                            System.out.print("Enter key: ");
                            String getKey = scanner.nextLine();
                            Response getResponse = client.sendRequest(new Request(Request.Type.GET, getKey, null));
                            System.out.println("Response: " + getResponse.getMessage());
                            break;

                        case "DELETE":
                            System.out.print("Enter key: ");
                            String deleteKey = scanner.nextLine();
                            Response deleteResponse = client.sendRequest(new Request(Request.Type.DELETE, deleteKey, null));
                            System.out.println("Response: " + deleteResponse.getMessage());
                            break;

                        default:
                            System.out.println("Unknown command");
                    }
                } catch (IOException e) {
                    logger.error("Error sending request: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            logger.error("Error in client: {}", e.getMessage());
        }
    }
} 