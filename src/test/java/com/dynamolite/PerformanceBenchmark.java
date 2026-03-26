package com.dynamolite;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

/**
 * Performance benchmarks for throughput and latency measurements.
 */
public class PerformanceBenchmark {
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

    @Test(timeout = 60000)
    public void benchmarkSingleNodeThroughput() throws Exception {
        node = startNode(7001);
        
        try (Client client = new Client(HOST, 7001)) {
            client.connect();
            
            // Warmup
            for (int i = 0; i < 50; i++) {
                client.sendRequest(new Request(Request.Type.PUT, "warmup_" + i, "value"));
            }
            
            // Actual benchmark
            int requests = 1000;
            long startMs = System.currentTimeMillis();
            
            for (int i = 0; i < requests; i++) {
                client.sendRequest(new Request(Request.Type.PUT, "benchKey_" + i, "value_" + i));
            }
            
            long elapsedMs = System.currentTimeMillis() - startMs;
            double writeRps = elapsedMs > 0 ? (requests * 1000.0) / elapsedMs : 0;
            
            // Benchmark reads
            long readStartMs = System.currentTimeMillis();
            for (int i = 0; i < requests; i++) {
                client.sendRequest(new Request(Request.Type.GET, "benchKey_" + (i % requests), null));
            }
            long readElapsedMs = System.currentTimeMillis() - readStartMs;
            double readRps = readElapsedMs > 0 ? (requests * 1000.0) / readElapsedMs : 0;
            
            System.out.println("========================================");
            System.out.println("SINGLE-NODE THROUGHPUT BENCHMARK");
            System.out.println("========================================");
            System.out.println("Total Write Requests: " + requests);
            System.out.println("Write Time: " + elapsedMs + "ms");
            System.out.println("Write RPS: " + String.format("%.2f", writeRps));
            System.out.println("----------------------------------------");
            System.out.println("Total Read Requests: " + requests);
            System.out.println("Read Time: " + readElapsedMs + "ms");
            System.out.println("Read RPS: " + String.format("%.2f", readRps));
            System.out.println("========================================");
            
            assertTrue("Should achieve at least 500 write RPS, got " + writeRps, writeRps > 500);
            assertTrue("Should achieve at least 600 read RPS, got " + readRps, readRps > 600);
        }
    }

    @Test(timeout = 60000)
    public void benchmarkLatencyPercentiles() throws Exception {
        node = startNode(7002);
        
        try (Client client = new Client(HOST, 7002)) {
            client.connect();
            
            // Warmup
            for (int i = 0; i < 20; i++) {
                client.sendRequest(new Request(Request.Type.PUT, "warmup_" + i, "value"));
            }
            
            // Measure write latencies
            int requests = 500;
            List<Long> writeLatenciesMs = new ArrayList<>(requests);
            
            for (int i = 0; i < requests; i++) {
                long lat = measureLatency(client, new Request(Request.Type.PUT, "latKey_" + i, "value_" + i));
                writeLatenciesMs.add(lat);
            }
            
            // Measure read latencies
            List<Long> readLatenciesMs = new ArrayList<>(requests);
            for (int i = 0; i < requests; i++) {
                long lat = measureLatency(client, new Request(Request.Type.GET, "latKey_" + (i % requests), null));
                readLatenciesMs.add(lat);
            }
            
            // Calculate percentiles
            Collections.sort(writeLatenciesMs);
            Collections.sort(readLatenciesMs);
            
            long writeP50 = writeLatenciesMs.get((int) (requests * 0.50));
            long writeP95 = writeLatenciesMs.get((int) (requests * 0.95));
            long writeP99 = writeLatenciesMs.get((int) (requests * 0.99));
            
            long readP50 = readLatenciesMs.get((int) (requests * 0.50));
            long readP95 = readLatenciesMs.get((int) (requests * 0.95));
            long readP99 = readLatenciesMs.get((int) (requests * 0.99));
            
            double writeAvg = writeLatenciesMs.stream().mapToLong(Long::longValue).average().orElse(0);
            double readAvg = readLatenciesMs.stream().mapToLong(Long::longValue).average().orElse(0);
            
            System.out.println("========================================");
            System.out.println("LATENCY PERCENTILES BENCHMARK");
            System.out.println("========================================");
            System.out.println("WRITE LATENCY:");
            System.out.println("  Average: " + String.format("%.2f", writeAvg) + "ms");
            System.out.println("  p50: " + writeP50 + "ms");
            System.out.println("  p95: " + writeP95 + "ms");
            System.out.println("  p99: " + writeP99 + "ms");
            System.out.println("----------------------------------------");
            System.out.println("READ LATENCY:");
            System.out.println("  Average: " + String.format("%.2f", readAvg) + "ms");
            System.out.println("  p50: " + readP50 + "ms");
            System.out.println("  p95: " + readP95 + "ms");
            System.out.println("  p99: " + readP99 + "ms");
            System.out.println("========================================");
            
            assertTrue("Write p99 should be under 200ms, got " + writeP99, writeP99 < 200);
            assertTrue("Read p99 should be under 200ms, got " + readP99, readP99 < 200);
            assertTrue("Write average should be under 120ms, got " + writeAvg, writeAvg < 120);
            assertTrue("Read average should be under 120ms, got " + readAvg, readAvg < 120);
        }
    }

    private long measureLatency(Client client, Request req) throws IOException {
        long start = System.nanoTime();
        client.sendRequest(req);
        return (System.nanoTime() - start) / 1_000_000; // Convert to ms
    }

    private Node startNode(int port) throws InterruptedException {
        String dataDir = "target/test_data/bench_node_" + port + "_" + System.currentTimeMillis();
        Node n = new Node(port, dataDir, 2, 2);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(n::start);
        executor.shutdown();
        Thread.sleep(500);
        return n;
    }
}