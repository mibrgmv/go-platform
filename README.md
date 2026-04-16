# go-platform

Shared Go library for reusable infrastructure components.

```
go-platform/
├── events/       — outbox-to-Kafka publisher and Kafka-to-inbox processor
├── inbox/        — idempotent event processing (inbox pattern)
├── kafka/        — Kafka producer and consumer
├── outbox/       — transactional outbox pattern
├── postgres/     — connection pool, transactions, migrations
├── ratelimiter/  — distributed token bucket (Redis + Lua)
└── workerpool/   — bounded worker pool with context cancellation
```
