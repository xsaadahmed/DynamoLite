# DynamoLite

A distributed key-value store implementing core concepts from Amazon's Dynamo paper — built from scratch in Java to understand what makes distributed systems actually work.

> Consistent hashing · Quorum replication · Vector clocks · Heartbeat failure detection

---

## CAP Theorem Position

DynamoLite is an **AP system** — it prioritizes Availability and Partition Tolerance over Consistency. During a network partition, nodes continue accepting writes to ensure service uptime. After the partition heals, replicas reconcile conflicting states using vector clocks.

---

## Design Decisions

### Why Eventual Consistency over Strong Consistency?
Went with a tunable quorum (N=3, R=2, W=2) rather than Raft or Paxos. In high-scale distributed systems, network partitions are inevitable. An AP system remains writable when nodes go down, making it the ideal choice for high-availability use cases like multi-user sessions or shopping carts. 

**Trade-off:** Clients may briefly read stale data until replicas converge (Eventual Consistency).

### Why Vector Clocks over Timestamps?
Wall-clock timestamps cannot distinguish between concurrent writes and sequential overwrites due to clock skew, often leading to silent data loss. Vector clocks capture causality, allowing the system to detect genuine conflicts that require resolution. 

**Trade-off:** Metadata overhead grows O(n) with the number of nodes, but for clusters under 100 nodes, this impact is negligible.

### Why Consistent Hashing?
Naive hashing ($key \pmod n$) causes massive data migration when a node joins or leaves. With consistent hashing, only $O(K/N)$ keys move. Using 3 virtual nodes per physical node smooths out the hash distribution, preventing "hotspots" and ensuring balanced CPU/Storage utilization.

### What Was Deliberately Left Out
- **Strong Consistency** — Would require a consensus algorithm like Raft, significantly increasing write latency.
- **Byzantine Fault Tolerance** — Assumes nodes fail but don't act maliciously (standard for internal infrastructure).
- **Automatic Rebalancing** — Node additions currently require manual migration; a gossip protocol would be the next logical iteration.
- **Ultra-Large Clusters** — While the architecture scales, the MD5 ring management becomes a bottleneck beyond ~50 nodes; a Jump Hash or specialized membership service would scale further.

---

## Architecture

```
Client
  │
  ▼
Coordinator Node  ──── ConsistentHashRing (MD5, 3 virtual nodes/physical)
  │                        │
  ├── Quorum Write ─────────┤──► Replica Node A
  │   (W=2 of N=3)          │──► Replica Node B
  │                         │──► Replica Node C
  └── Quorum Read ──────────┘
      (R=2 of N=3)

HealthMonitor (background thread)
  └── Heartbeat every 1s → marks node failed after 3s → updates hash ring
```

### Core Components

| Component | Function |
|---|---|
| `ConsistentHashRing` | MD5-based ring with virtual nodes for uniform data distribution |
| `Node` | Coordinates quorum operations, handles RPC, and manages replication |
| `Storage` | Thread-safe key-value store with versioned entries and disk persistence |
| `VersionVector` | Implements vector clocks to track causality and detect write conflicts |
| `HealthMonitor` | Failure detector — triggers eviction after 3s of heartbeat silence |
| `NodeConnection` | Managed TCP RPC layer with automatic retries and timeout handling |

---

## Performance

Benchmarked on local loopback with 10 concurrent clients:

| Operation | Peak Throughput | Success Rate | Avg Latency |
|---|---|---|---|
| **Writes** | **11,000 RPS** | 100% | ~12ms |
| **Reads** | **32,000 RPS** | 100% | ~3ms |

*Note: Multi-node throughput over a real network is typically 20-30% of local loopback speeds due to serialization overhead and TCP congestion control across physical interfaces.*

---

## Test Suite

```
33 tests · 0 failures · 0 errors
```

| Suite | Tests | Verifies |
|---|---|---|
| `DynamoLiteTest` | 3 | Core component unit logic |
| `CoverageBoostTest` | 25 | Error paths, edge cases, and network failure scenarios |
| `IntegrationTest` | 3 | Multi-node replication and single-node failure recovery |
| `ConcurrentLoadTest` | 2 | Sustained throughput under high-concurrency load |

**Coverage:** 66% Line / 55% Branch (verified via JaCoCo)

---

## Configuration

```java
// Node.java — Quorum Parameters
int replicationFactor = 3;  // N: total replicas per key
int readQuorum  = 2;         // R: responses needed for a read
int writeQuorum = 2;         // W: acks needed for a write

// HealthMonitor.java — Failure Detection
long HEARTBEAT_INTERVAL_MS = 1000;  // ping frequency
long FAILURE_THRESHOLD_MS  = 3000;  // silence threshold before eviction
```

---

## Project Structure

```
src/main/java/com/dynamolite/
├── Node.java                # Coordinator logic and quorum management
├── ConsistentHashRing.java  # MD5 distribution ring
├── Storage.java             # Versioned KV engine
├── VersionVector.java       # Causality tracking (Vector Clocks)
├── NodeConnection.java      # TCP RPC layer with retry logic
├── HealthMonitor.java       # Heartbeat-based failure detection
├── Client.java              # Interactive CLI
├── Request.java             # Messaging protocol (Request)
└── Response.java            # Messaging protocol (Response)
```

---

## Quick Start

**Prerequisites:** Java 11+, Maven 3.6+

### Build and Test
```bash
mvn clean install
mvn clean test
mvn jacoco:report
# Report: target/site/jacoco/index.html
```

### Run a 3-Node Cluster
```bash
# Open three separate terminals
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node 5001
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node 5002
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node 5003
```

### Interactive Client
```bash
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Client localhost 5001
```

---

## Troubleshooting

**Port already in use**
- **Windows:** `netstat -ano | findstr :5001` then `taskkill /PID <PID> /F`
- **Linux/Mac:** `lsof -ti:5001 | xargs kill -9`

**Tests timing out:** Integration tests require ports 5001–5003 and 6001–6002 to be available for cluster simulation.

---

## Further Reading

- [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Martin Kleppmann
- [Distributed Systems for Fun and Profit](http://book.mixu.net/distsys/)

---

*Educational implementation. For production, see [Cassandra](https://cassandra.apache.org/), [Riak](https://riak.com/), or [DynamoDB](https://aws.amazon.com/dynamodb/).*

