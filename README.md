# Transactional Outbox in .NET — never lose an event again

[![Build](https://github.com/varunkonnojutech-lab/outbox-pattern/actions/workflows/ci.yml/badge.svg)](https://github.com/varunkonnojutech-lab/outbox-pattern/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![.NET 8](https://img.shields.io/badge/.NET-8.0-512BD4)](https://dotnet.microsoft.com/)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/varunkonnojutech-lab/outbox-pattern)

Your service saves an order to the database, then publishes an `OrderPlaced` event to a broker. Two separate writes — and sooner or later one of them fails. The order saves but the event never fires, so downstream systems never hear about it. Or the event fires but the save rolls back, so downstream systems act on an order that does not exist. This is the **dual-write problem**, and it is the bug behind a whole class of "the email never arrived / the inventory never updated" incidents.

The transactional outbox fixes it with one idea: **write the event to the same database, in the same transaction, as the business change.** A separate relay worker then publishes outbox rows to the broker. Nothing is ever lost, and nothing is ever announced before it happened.

## Who is this for

.NET developers building microservices or any system where one database write must reliably notify another system — order → inventory, payment → notification, user signup → welcome email. If you have ever written `SaveChangesAsync()` followed by `PublishAsync()` and hoped both succeed, this is the repo.

## What you'll learn

- **The dual-write problem** — why save-then-publish (and publish-then-save) both fail, and what each failure looks like in production.
- **The outbox table** — one row per event, written atomically with the business change, so the event and the change share a single fate.
- **The relay worker** — polling for unpublished rows, publishing oldest-first, and marking rows published only after a successful publish.
- **At-least-once delivery** — why a crash between publish and marking published is safe, why duplicates are therefore possible, and why consumers must be idempotent.
- **Failure handling** — a failed publish leaves the row unpublished with a retry count and error, so the next pass retries it instead of losing it. Stuck rows are visible in the table, not invisible in a log file.
- **What this pattern does not solve** — ordering across aggregates, exactly-once delivery (it does not exist over a network), and the relay's polling cost at very high volume (where change-data-capture tools like Debezium take over).

## Quick start

You need .NET 8. No Docker, no broker, no external services — the demo uses SQLite and an in-memory bus:

```bash
dotnet test
dotnet run --project src/OutboxPattern.Demo
```

## What the demo shows

1. **Place** — two orders are saved; each save writes the order row *and* its outbox row in one transaction.
2. **Relay pass 1** — the "broker" is down for the first message. The publish fails, the row stays unpublished, and nothing is lost.
3. **Relay pass 2** — the broker is back. The failed message is retried and everything publishes.
4. **Relay pass 3** — already-published rows are never re-sent, and the delivered count proves no duplicates slipped through.

## The pattern, in one picture

```
Place order
    │
    ▼
┌──────────────────────────────┐   one transaction
│  Orders table:  new order    │ ◄──────────────────┐
│  Outbox table:  OrderPlaced  │                    │ commit together
└──────────────┬───────────────┘   or not at all ───┘
               │
               ▼
        Relay worker (polls unpublished rows, oldest first)
               │
               ▼
        Message broker / webhook / downstream service
               │
               ▼
        Row marked published — only after a successful publish
```

And the failure cases, each handled:

| What goes wrong | What happens |
|---|---|
| Order save fails | Outbox row rolls back with it — no phantom event |
| Broker down during relay | Row stays unpublished, retry count grows, next pass retries |
| Relay crashes after publishing, before marking published | Row publishes again next pass — consumers deduplicate by message Id |
| Relay crashes before publishing | Row is still unpublished — next pass picks it up |

## The pieces

| File | Role |
|---|---|
| `src/OutboxPattern.Core/OrderService.cs` | Places an order: order + outbox row in a single `SaveChangesAsync` |
| `src/OutboxPattern.Core/OutboxMessage.cs` | The outbox row: type, JSON payload, published timestamp, retry count |
| `src/OutboxPattern.Core/OutboxRelay.cs` | Polls unpublished rows oldest-first, publishes, marks published on success only |
| `src/OutboxPattern.Core/MessageBus.cs` | `IMessageBus` abstraction, in-memory bus with delivery dedup, and a flaky bus for failure demos |
| `src/OutboxPattern.Core/AppDbContext.cs` | EF Core + SQLite context for orders and outbox rows |

## Production notes

- **Polling interval** is a trade-off: seconds of latency vs. database load. Start at 1–5 seconds and measure.
- **Claim rows before publishing** (e.g. a `LockedUntil` timestamp) once you run more than one relay instance, so two relays cannot publish the same row concurrently.
- **Dead-letter stuck rows**: alert when `RetryCount` passes a threshold or a row stays unpublished past an age limit — that is a bug or an outage, not noise.
- **Ordering**: this relay preserves order per database, not per aggregate. If per-aggregate order matters, publish in `CreatedAt` order per aggregate key, or use a single-partition topic keyed by aggregate id.
- **When to graduate**: at very high throughput, replace polling with change-data-capture (Debezium reading the transaction log) — same outbox table, no polling cost.

## Next steps

- Add a `LockedUntil` claim column and run two relay instances against one database.
- Swap `InMemoryMessageBus` for a real broker (RabbitMQ, Kafka, Azure Service Bus) behind the same `IMessageBus` interface — the relay and tests do not change.
- Pair this with the saga pattern (coming next in this series): the outbox guarantees the event leaves, the saga guarantees the multi-service workflow completes.
