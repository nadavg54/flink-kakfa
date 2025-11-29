# Lessons Learned

This document captures the problems we encountered during development and how we solved them.

## 1. Kafka Replication Factor Mismatch

### Problem
Kafka logs showed repeated attempts to create `__consumer_offsets` topic that never succeeded. Consumer groups were failing because the offsets topic couldn't be created.

### Root Cause
The docker-compose configuration had a mismatch:
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"` was set
- But initially only one broker was configured/running
- Kafka couldn't create the `__consumer_offsets` topic because it required 3 replicas but only 1 broker was available

### Solution
Ensure the replication factor matches the number of available brokers:
- If running 3 brokers: `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "3"` ✅
- If running 1 broker: `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"` ✅

After ensuring all 3 brokers were running and properly configured, the offsets topic was created successfully.

### Key Takeaway
Replication factor must not exceed the number of available brokers. Kafka cannot create topics with replication factor higher than available brokers.

---

## 2. Kafka Controller Architecture

### Problem
Initial confusion about what the "controller" is - is it a Kafka component or Go library jargon?

### Explanation
The **controller** is a core Kafka component:
- One broker in the cluster is elected as the controller
- Manages cluster metadata (topics, partitions, brokers)
- Handles administrative operations (topic creation/deletion)
- Coordinates leader elections when brokers fail
- **Only the controller can create/delete topics**

### How It Works in Tests
1. Connect to any Kafka broker
2. Ask: "Which broker is the controller?"
3. Connect directly to the controller broker
4. Create topics through the controller

### Key Takeaway
Understanding Kafka's architecture (controller, brokers, partitions) is essential for proper integration.

---

## 3. Docker Port Mapping and Advertised Listeners

### Problem
Tests were failing with connection errors when trying to connect to the Kafka controller:
```
failed to dial: failed to open connection to localhost:19093: connection refused
```

### Root Cause
Kafka was advertising container ports instead of host-mapped ports:
- **Port mapping**: `9093:19093` (host:container)
- **Advertised listener**: `PLAINTEXT_HOST://localhost:19093` ❌ (container port)
- **Should be**: `PLAINTEXT_HOST://localhost:9093` ✅ (host port)

When Kafka returned controller metadata, it advertised port `19093` (container port), but clients connecting from outside Docker need port `9093` (host-mapped port).

### Solution
Updated `KAFKA_ADVERTISED_LISTENERS` in docker-compose to use host ports:
```yaml
# Before
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092,PLAINTEXT_HOST://localhost:19093

# After
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092,PLAINTEXT_HOST://localhost:9093
```

### Key Takeaway
When running Kafka in Docker, `KAFKA_ADVERTISED_LISTENERS` must use the **host ports** that clients will use, not the container-internal ports.

---

## 4. Test Structure and Best Practices

### Problem
Initial test was one large function doing everything.

### Solution
Broke down into helper functions:
- `ensureTestTopicExists()` - Sets up test infrastructure
- `createTestKafkaWriter()` - Creates producer for testing
- `createTestKafkaReader()` - Creates consumer with fallback
- `sendTestMessages()` - Sends test data
- `readMessagesUntilDeadline()` - Reads with timeout
- `verifyAllMessagesReceived()` - Validates results

### Key Practices Learned
- Use unique consumer group IDs per test run to avoid conflicts
- Handle transient errors with retries
- Set appropriate timeouts
- Clean up resources (defer Close())
- Test both success and failure paths

### Key Takeaway
Well-structured tests are:
- Easier to understand
- Easier to maintain
- More reliable (handle edge cases)
- Self-documenting

---

## 8. Kafka Topic Creation in Tests

### Problem
Tests need topics to exist, but we can't assume they're already created.

### Solution
Tests create topics programmatically:
1. Connect to any broker
2. Find the controller broker
3. Connect to controller
4. Create topic with appropriate settings (partitions, replication)

### Key Takeaway
Integration tests should be self-contained and set up their own infrastructure when possible.

---

## Summary

The main challenges were:
1. **Replication factor mismatch** - Ensuring replication factor matches available brokers
2. **Docker networking** - Understanding port mapping and advertised listeners
3. **Kafka architecture** - Learning about controllers, consumer groups, and topics
4. **Test reliability** - Handling Kafka initialization and transient errors

These lessons will be valuable as we continue building the Flink integration and end-to-end tests.

