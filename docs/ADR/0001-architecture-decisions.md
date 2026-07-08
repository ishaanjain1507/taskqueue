# ADR 0001: Architecture and Tech Stack Decisions

## Status
Accepted

## Context
We are building a highly scalable, asynchronous background job processing system. The system needs to ingest jobs rapidly, process them concurrently across horizontal nodes, and provide strong guarantees that no jobs are lost. 

## Decisions

### 1. Go for the Core System
**Decision:** Use Go (Golang) for both the API layer and the Worker pool.
**Rationale:**
- Concurrency model: Go's goroutines make it trivial to spin up a dynamic worker pool where each worker processes jobs independently.
- Performance: Compiled, garbage-collected language capable of handling massive throughput with very low memory overhead.

### 2. Redis as the Message Broker
**Decision:** Use Redis `LPUSH` and `BRPOP` for the job queue.
**Rationale:**
- **Why not RabbitMQ or Kafka?** Redis is significantly lighter, easier to host, and provides incredibly fast list operations. `BRPOP` guarantees that workers are completely idle (0 CPU usage) when the queue is empty, eliminating the need for CPU-heavy polling loops. 

### 3. PostgreSQL as the Source of Truth
**Decision:** Use Postgres to track job state (Pending, Processing, Success, Failed, Dead) instead of keeping all state in Redis.
**Rationale:**
- **Durability:** While Redis can be configured for persistence, PostgreSQL provides robust ACID guarantees for critical state transitions.
- **Queryability:** Tracking down failed jobs, aggregating metrics, or filtering by custom job types is far easier and more efficient in a relational database than scanning Redis keys.

### 4. Circuit Breakers for Database Calls
**Decision:** Implement `gobreaker` around PostgreSQL Upsert queries.
**Rationale:**
- If the database struggles under load, the circuit breaker trips, causing worker goroutines to fail fast instead of piling up and crashing the Go application due to memory exhaustion. This protects downstream infrastructure and ensures the system degrades gracefully.
