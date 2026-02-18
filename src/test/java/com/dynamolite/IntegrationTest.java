package com.dynamolite;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * Integration tests for connectivity, replication, and failure recovery.
 */
public class IntegrationTest {
    private static final String HOST = "localhost";
    private Node node1;
    private Node node2;
    private Node node3;
    private Client client;

    @Before
    public void setUp() {
        Node.setHealthCheckEnabled(false);
    }

    @After
    public void tearDown() throws InterruptedException {
        Node.setHealthCheckEnabled(true);
        if (node1 != null) { node1.stop(); node1 = null; }
        if (node2 != null) { node2.stop(); node2 = null; }
        if (node3 != null) { node3.stop(); node3 = null; }
        if (client != null) {
            try { client.close(); } catch (Exception e) { /* ignore */ }
            client = null;
        }
        Thread.sleep(1000); // Wait for ports to be released
    }

    @Test(timeout = 15000)
    public void testHeartbeatResponse() {
        try {
            node1 = startNode(5001);
            client = new Client(HOST, 5001);
            client.connect();
            Response response = client.sendRequest(new Request(Request.Type.HEARTBEAT, null, null));
            assertTrue("Heartbeat should succeed", response.isSuccess());
            assertEquals("PONG", response.getMessage());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        }
    }

    @Test(timeout = 20000)
    public void testBasicReplication() {
        try {
            node1 = startNode(5001);
            node2 = startNode(5002);
            node3 = startNode(5003);

            Thread.sleep(800);

            node1.addPeer(node2.getNodeId(), 5002);
            node1.addPeer(node3.getNodeId(), 5003);
            node2.addPeer(node1.getNodeId(), 5001);
            node2.addPeer(node3.getNodeId(), 5003);
            node3.addPeer(node1.getNodeId(), 5001);
            node3.addPeer(node2.getNodeId(), 5002);

            client = new Client(HOST, 5001);
            client.connect();
            Response putResponse = client.sendRequest(new Request(Request.Type.PUT, "testKey", "testValue"));
            assertTrue("PUT should succeed", putResponse.isSuccess());

            Response getResponse = client.sendRequest(new Request(Request.Type.GET, "testKey", null));
            assertTrue("GET should succeed", getResponse.isSuccess());
            assertNotNull(getResponse.getMessage());
            assertTrue("Response should contain testValue", getResponse.getMessage().contains("testValue"));
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        }
    }

    @Test(timeout = 30000)
    public void testNodeFailureRecovery() {
        try {
            node1 = startNode(5001);
            node2 = startNode(5002);
            node3 = startNode(5003);

            Thread.sleep(800);

            node1.addPeer(node2.getNodeId(), 5002);
            node1.addPeer(node3.getNodeId(), 5003);
            node2.addPeer(node1.getNodeId(), 5001);
            node2.addPeer(node3.getNodeId(), 5003);
            node3.addPeer(node1.getNodeId(), 5001);
            node3.addPeer(node2.getNodeId(), 5002);

            try (Client c = new Client(HOST, 5001)) {
                c.connect();
                Response putResponse = c.sendRequest(new Request(Request.Type.PUT, "key1", "value1"));
                assertTrue("PUT should succeed", putResponse.isSuccess());
            }

            node1.stop();
            node1 = null; // tearDown will not stop again
            Thread.sleep(4000);

            try (Client c = new Client(HOST, 5002)) {
                c.connect();
                Response getResponse = c.sendRequest(new Request(Request.Type.GET, "key1", null));
                assertTrue("GET after failure should succeed", getResponse.isSuccess());
                assertNotNull(getResponse.getMessage());
                assertTrue("Should read value1", getResponse.getMessage().contains("value1"));
            }
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        }
    }

    private Node startNode(int port) throws InterruptedException {
        String dataDir = "target/test_data/node_" + port + "_" + System.currentTimeMillis();
        Node node = new Node(port, dataDir, 2, 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(node::start);
        executor.shutdown();
        Thread.sleep(500);
        return node;
    }
}
