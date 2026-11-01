namespace OutboxPattern.Core;

/// <summary>
/// The transport an outbox message is published to: a message broker, a
/// webhook, another service's API. The relay does not care which — it only
/// needs a Publish that throws on failure, so a failed publish stays
/// unpublished and gets retried.
/// </summary>
public interface IMessageBus
{
    Task PublishAsync(OutboxMessage message, CancellationToken ct = default);
}

/// <summary>
/// In-memory stand-in for a real broker, for the demo and the tests.
/// Delivery is at-least-once, as with real brokers: if the relay publishes
/// twice, the bus deduplicates on the message Id, and consumers must
/// therefore be idempotent too.
/// </summary>
public sealed class InMemoryMessageBus : IMessageBus
{
    private readonly HashSet<Guid> _seen = new();
    public List<OutboxMessage> Delivered { get; } = new();
    public int DuplicateDeliveriesIgnored { get; private set; }

    public Task PublishAsync(OutboxMessage message, CancellationToken ct = default)
    {
        if (!_seen.Add(message.Id))
        {
            DuplicateDeliveriesIgnored++;
            return Task.CompletedTask;
        }

        Delivered.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Wraps another bus and fails the first <paramref name="failures"/> publish
/// attempts, to demonstrate that a failed publish is retried, not lost.
/// </summary>
public sealed class FlakyMessageBus : IMessageBus
{
    private readonly IMessageBus _inner;
    private readonly int _failures;
    private int _attempts;

    public FlakyMessageBus(IMessageBus inner, int failures = 1)
    {
        _inner = inner;
        _failures = failures;
    }

    public Task PublishAsync(OutboxMessage message, CancellationToken ct = default)
    {
        if (Interlocked.Increment(ref _attempts) <= _failures)
            throw new InvalidOperationException("Simulated broker outage.");
        return _inner.PublishAsync(message, ct);
    }
}
