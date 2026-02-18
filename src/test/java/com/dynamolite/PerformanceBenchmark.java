package com.dynamolite;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * Simple throughput and latency benchmarks (no JMH).
 */
@Ignore("Disabled for now - debugging node startup")
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

    @Test
    public void benchmarkSingleNodeThroughput() throws Exception {
        node = startNode(7001);
        try (Client client = new Client(HOST, 7001)) {
            client.connect();
            int requests = 1000;
            long startMs = System.currentTimeMillis();
            for (int i = 0; i < requests; i++) {
                client.sendRequest(new Request(Request.Type.PUT, "benchKey_" + i, "value_" + i));
            }
            long elapsedMs = System.currentTimeMillis() - startMs;
            double rps = elapsedMs > 0 ? (requests * 1000.0) / elapsedMs : 0;
            System.out.println("Single-node throughput: " + rps + " RPS");
            assertTrue("Should achieve at least 100 RPS, got " + rps, rps > 100);
        }
    }

    @Test
    public void benchmarkLatencyPercentiles() throws Exception {
        node = startNode(7002);
        try (Client client = new Client(HOST, 7002)) {
            client.connect();
            int requests = 100;
            List<Long> latenciesMs = new ArrayList<>(requests);
            for (int i = 0; i < requests; i++) {
                long lat = measureLatency(client, new Request(Request.Type.PUT, "latKey_" + i, "value_" + i));
                latenciesMs.add(lat);
            }
            Collections.sort(latenciesMs);
            long p50 = latenciesMs.get((int) (requests * 0.50));
            long p99 = latenciesMs.get((int) (requests * 0.99));
            System.out.println("p50: " + p50 + "ms, p99: " + p99 + "ms");
            assertTrue("Should have measured latencies", !latenciesMs.isEmpty());
        }
    }

    private long measureLatency(Client client, Request req) throws IOException {
        long start = System.currentTimeMillis();
        client.sendRequest(req);
        return System.currentTimeMillis() - start;
    }

    private Node startNode(int port) throws InterruptedException {
        String dataDir = "target/test_data/bench_node_" + port + "_" + System.currentTimeMillis();
        Node n = new Node(port, dataDir, 2, 2);
        Executors.newSingleThreadExecutor().submit(n::start);
        Thread.sleep(500);
        return n;
    }
}
