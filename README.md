# DynamoLite

A distributed key-value store implementing core concepts from Amazon's Dynamo paper — built from scratch in Java to understand what makes distributed systems actually work.

> Consistent hashing · Quorum replication · Vector clocks · Heartbeat failure detection

---

## Architecture

```
Client
  │
  ▼
Coordinator Node  ──── ConsistentHashRing (MD5, 3 virtual nodes/physical)
  │                         │
  ├── Quorum Write ─────────┤──► Replica Node A
  │   (W=2 of N=3)          │──► Replica Node B
  │                         │──► Replica Node C
  └── Quorum Read ──────────┘
      (R=2 of N=3)

HealthMonitor (background thread)
  └── Heartbeat every 1s → marks node failed after 3s → updates hash ring
```

### Core Components

| Component | What It Does |
|---|---|
| `ConsistentHashRing` | MD5-based ring with 3 virtual nodes per physical node for balanced distribution |
| `Node` | Handles client requests, coordinates quorum reads/writes, manages replication |
| `Storage` | Thread-safe key-value store with versioned entries, persisted to disk |
| `VersionVector` | Tracks causality between concurrent writes for conflict detection |
| `HealthMonitor` | Background thread — heartbeats every 1s, failure threshold 3s |
| `NodeConnection` | Serialized RPC over TCP sockets with timeout handling |

---

## Performance

Benchmarked on a single-node setup with 10 concurrent clients, 1,000 requests each:

| Operation | Throughput | Success Rate |
|---|---|---|
| **Writes** | **7,692 RPS** | 100% |
| **Reads** | **20,000 RPS** | 100% |

Multi-node throughput is lower due to quorum coordination overhead (waiting for W=2 / R=2 responses across the network).

---

## Design Decisions

### Why eventual consistency over strong consistency?
Went with tunable quorum (N=3, R=2, W=2) rather than Raft/Paxos. Network partitions are inevitable — an AP system stays writable when nodes go down, which is the right call for use cases like session storage or shopping carts. Trade-off: clients may read stale data until replicas converge.

### Why vector clocks over timestamps?
Timestamps can't distinguish between concurrent writes and sequential overwrites — you silently lose data. Vector clocks capture causality, so the system can detect genuine conflicts instead of guessing. The cost is O(n) clock size per entry, but for 3–10 nodes that's negligible.

### Why consistent hashing?
When a node joins or leaves, only O(K/N) keys move instead of O(K) for naive hashing. The 3 virtual nodes per physical node smooth out load distribution and prevent hotspots from hash skew.

### What was deliberately left out
- **Strong consistency** — would require Raft/Paxos, adds significant complexity and latency
- **Byzantine fault tolerance** — assumes non-malicious failures (appropriate for internal infrastructure)
- **Automatic rebalancing** — node additions require manual key migration; could be solved with a gossip protocol
- **Large clusters** — consistent hashing ring becomes a bottleneck beyond ~10 nodes; jump hash would scale better

---

## CAP Theorem Position

DynamoLite is an **AP system** — it prioritizes Availability and Partition Tolerance over Consistency. During a network partition, nodes continue accepting writes. After the partition heals, replicas reconcile using vector clocks.

---

## Test Suite

```
33 tests · 0 failures · 0 errors
```

| Suite | Tests | What It Verifies |
|---|---|---|
| `DynamoLiteTest` | 3 | Core component unit tests |
| `CoverageBoostTest` | 25 | Error paths, edge cases, failure scenarios |
| `IntegrationTest` | 3 | Multi-node replication and single-node failure recovery |
| `ConcurrentLoadTest` | 2 | Throughput under concurrent client load |

Coverage: **60% line / 42% branch** (HealthMonitor concurrency paths are the main gap)

---

## Quick Start

**Prerequisites:** Java 11+, Maven 3.6+

```bash
# Build
mvn clean install

# Run all tests + coverage report
mvn clean test
mvn jacoco:report
# open target/site/jacoco/index.html
```

### Start a 3-node cluster

```bash
# Three separate terminals
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node 5001
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node 5002
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node 5003
```

### Client

```bash
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Client localhost 5001
```

```
PUT key: session:user42   value: {"cart": [1, 2, 3]}
GET key: session:user42
DELETE key: session:user42
QUIT
```

---

## Configuration

```java
// Node.java — quorum parameters
int replicationFactor = 3;  // N: total replicas per key
int readQuorum  = 2;         // R: responses needed for a read
int writeQuorum = 2;         // W: acks needed for a write

// HealthMonitor.java — failure detection
long HEARTBEAT_INTERVAL_MS = 1000;  // ping frequency
long FAILURE_THRESHOLD_MS  = 3000;  // silence before marking dead
```

---

## Project Structure

```
src/main/java/com/dynamolite/
├── Node.java                # Core server: client handling, quorum coordination
├── ConsistentHashRing.java  # MD5 ring with virtual nodes
├── Storage.java             # Thread-safe versioned key-value store
├── VersionVector.java       # Vector clock implementation
├── NodeConnection.java      # TCP RPC with serialization and timeouts
├── HealthMonitor.java       # Background heartbeat + failure detection
├── Client.java              # CLI client
├── Request.java             # Wire protocol (request)
└── Response.java            # Wire protocol (response)
```

---

## Troubleshooting

**Port already in use**
```bash
# Windows
netstat -ano | findstr :5001 && taskkill /PID <PID> /F

# Linux/Mac
lsof -ti:5001 | xargs kill -9
```

**Tests are slow / timing out** — Integration tests start real nodes and wait for quorum across them. Ports 5001–5003 and 6001–6002 must be free before running.

---

## Further Reading

- [Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) — the paper this implements
- [Designing Data-Intensive Applications](https://dataintensive.net/) — Chapter 5 (Replication) is especially relevant
- [Distributed Systems for Fun and Profit](http://book.mixu.net/distsys/)

---

*Educational implementation. For production, see [Cassandra](https://cassandra.apache.org/), [Riak](https://riak.com/), or [DynamoDB](https://aws.amazon.com/dynamodb/).*