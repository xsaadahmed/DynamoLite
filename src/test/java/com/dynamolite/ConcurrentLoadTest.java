package com.dynamolite;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Concurrent load tests for write and read throughput.
 */
public class ConcurrentLoadTest {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentLoadTest.class);
    private static final String HOST = "localhost";
    private Node node;

    @Before
    public void setUp() {
        Node.setHealthCheckEnabled(false);
    }

    @After
    public void tearDown() throws InterruptedException {
        Node.setHealthCheckEnabled(true);
        if (node != null) {
            node.stop();
            node = null;
        }
        Thread.sleep(1000); // Wait for ports to be released
    }

    @Test(timeout = 35000)
    public void testConcurrentWrites() throws Exception {
        // Use single-node setup (no replication) to avoid inter-node communication issues
        String dataDir = "target/test_data/load_node_6001_" + System.currentTimeMillis();
        node = new Node(6001, dataDir, 1, 1); // quorum=1 (no replication needed)
        node.setPersistenceEnabled(false); // Disable disk I/O for performance testing
        ExecutorService startExecutor = Executors.newSingleThreadExecutor();
        startExecutor.submit(node::start);
        startExecutor.shutdown();
        Thread.sleep(1000); // Give more time for node to start
        
        int threadCount = 10;  
        int requestsPerThread = 100;  
        int totalRequests = threadCount * requestsPerThread;
        
        CountDownLatch latch = new CountDownLatch(totalRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        long startMs = System.currentTimeMillis();
        
        // Reuse connections per thread to avoid socket exhaustion
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                Client c = null;
                try {
                    c = new Client(HOST, 6001);
                    c.connect();
                    for (int i = 0; i < requestsPerThread; i++) {
                        try {
                            Response resp = c.sendRequest(new Request(Request.Type.PUT, "key_" + threadId + "_" + i, "value_" + i));
                            if (resp.isSuccess()) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        } catch (IOException e) {
                            failureCount.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    }
                } catch (IOException e) {
                    logger.error("Connection failed for thread {}", threadId, e);
                    // Count down remaining for this thread
                    long remaining = latch.getCount();
                    for (int i = 0; i < Math.min(requestsPerThread, remaining); i++) {
                        failureCount.incrementAndGet();
                        latch.countDown();
                    }
                } finally {
                    if (c != null) {
                        try {
                            c.close();
                        } catch (Exception ignored) {}
                    }
                }
            });
        }
        
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        long elapsedMs = System.currentTimeMillis() - startMs;
        
        // Force shutdown
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        assertTrue("Test should complete within timeout", completed);
        
        double rps = elapsedMs > 0 ? (successCount.get() * 1000.0) / elapsedMs : 0;
        System.out.println("========================================");
        System.out.println("CONCURRENT WRITE TEST RESULTS");
        System.out.println("========================================");
        System.out.println("Total Requests: " + totalRequests);
        System.out.println("Successful: " + successCount.get());
        System.out.println("Failed: " + failureCount.get());
        System.out.println("Elapsed Time: " + elapsedMs + "ms");
        System.out.println("Write RPS: " + String.format("%.2f", rps));
        System.out.println("========================================");
        
        assertTrue("Should achieve at least 400 write RPS, got " + rps, rps > 400);
        assertTrue("Should have at least 80% success rate", successCount.get() > totalRequests * 0.8);
    }

    @Test(timeout = 35000)
    public void testConcurrentReads() throws Exception {
        // Use single-node setup
        String dataDir = "target/test_data/load_node_6002_" + System.currentTimeMillis();
        node = new Node(6002, dataDir, 1, 1);
        node.setPersistenceEnabled(false); // Disable disk I/O for performance testing
        ExecutorService startExecutor = Executors.newSingleThreadExecutor();
        startExecutor.submit(node::start);
        startExecutor.shutdown();
        Thread.sleep(1000); // Give more time for node to start
        
        // Pre-populate 100 keys
        try (Client client = new Client(HOST, 6002)) {
            client.connect();
            for (int i = 0; i < 100; i++) {
                client.sendRequest(new Request(Request.Type.PUT, "key_" + i, "value_" + i));
            }
        }

        int threadCount = 10;
        int readsPerThread = 100;
        int totalReads = threadCount * readsPerThread;
        
        CountDownLatch latch = new CountDownLatch(totalReads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        long startMs = System.currentTimeMillis();
        
        // Reuse connections per thread
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                Client c = null;
                try {
                    c = new Client(HOST, 6002);
                    c.connect();
                    for (int i = 0; i < readsPerThread; i++) {
                        try {
                            Response resp = c.sendRequest(new Request(Request.Type.GET, "key_" + (i % 100), null));
                            if (resp.isSuccess()) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }
                        } catch (IOException e) {
                            failureCount.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    }
                } catch (IOException e) {
                    // Connection failed
                    long remaining = latch.getCount();
                    for (int i = 0; i < Math.min(readsPerThread, remaining); i++) {
                        failureCount.incrementAndGet();
                        latch.countDown();
                    }
                } finally {
                    if (c != null) {
                        try {
                            c.close();
                        } catch (Exception ignored) {}
                    }
                }
            });
        }
        
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        long elapsedMs = System.currentTimeMillis() - startMs;
        
        // Force shutdown
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        assertTrue("Test should complete within timeout", completed);
        
        double rps = elapsedMs > 0 ? (successCount.get() * 1000.0) / elapsedMs : 0;
        System.out.println("========================================");
        System.out.println("CONCURRENT READ TEST RESULTS");
        System.out.println("========================================");
        System.out.println("Total Requests: " + totalReads);
        System.out.println("Successful: " + successCount.get());
        System.out.println("Failed: " + failureCount.get());
        System.out.println("Elapsed Time: " + elapsedMs + "ms");
        System.out.println("Read RPS: " + String.format("%.2f", rps));
        System.out.println("========================================");
        
        assertTrue("Should achieve at least 1000 read RPS, got " + rps, rps > 1000);
        assertTrue("Should have at least 90% success rate", successCount.get() > totalReads * 0.9);
    }
}