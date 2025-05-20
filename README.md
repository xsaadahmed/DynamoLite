# DynamoLite

A lightweight distributed key-value store inspired by Amazon's DynamoDB architecture. This project demonstrates key concepts in distributed systems, including consistent hashing, quorum-based operations, and fault tolerance.

## Features

- **Consistent Hashing**: Efficient key distribution across nodes
- **Quorum-based Operations**: Eventual consistency with configurable read/write quorums
- **File-based Persistence**: Durable storage with version vectors
- **Replication & Failure Recovery**: Automatic data replication and node failure handling
- **Concurrent Request Processing**: Thread-safe operations with proper synchronization

## Architecture

The system consists of the following main components:

1. **Node**: Represents a single server in the distributed system
2. **Storage**: Handles data persistence and retrieval
3. **ConsistentHashRing**: Manages node distribution and key routing
4. **VersionVector**: Tracks data versions for conflict resolution
5. **Client**: Interface for interacting with the system

## Building and Running

### Prerequisites

- Java 11 or higher
- Maven 3.6 or higher

### Building

```bash
mvn clean install
```

### Running

To start a node:
```bash
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Node <port>
```

To start the client:
```bash
java -cp target/dynamolite-1.0-SNAPSHOT.jar com.dynamolite.Client <host> <port>
```

## API

The system supports the following operations:

- `PUT(key, value)`: Store a key-value pair
- `GET(key)`: Retrieve a value by key
- `DELETE(key)`: Remove a key-value pair

## Implementation Details

### Consistent Hashing
- Uses a ring-based approach for node distribution
- Virtual nodes for better load balancing
- Automatic node addition/removal handling

### Quorum Operations
- Configurable read/write quorum sizes
- Eventual consistency model
- Conflict resolution using version vectors

### Fault Tolerance
- Automatic node failure detection
- Data replication across nodes
- Recovery mechanisms for failed nodes

## Testing

Run the test suite:
```bash
mvn test
```

## License

MIT License 