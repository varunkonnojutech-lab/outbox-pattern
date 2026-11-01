namespace OutboxPattern.Core;

/// <summary>
/// One row per event that must reach another system. The row is written in
/// the SAME database transaction as the business change it describes, so
/// the event can never be lost (no dual write) and can never announce a
/// change that did not happen.
/// </summary>
public sealed class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();

    /// <summary>Event type, e.g. "OrderPlaced". Consumers route on this.</summary>
    public string Type { get; set; } = string.Empty;

    /// <summary>JSON payload of the event.</summary>
    public string Payload { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    /// <summary>Null until the relay has successfully published this message.</summary>
    public DateTime? PublishedAt { get; set; }

    /// <summary>How many publish attempts have failed. Used for backoff/alerting.</summary>
    public int RetryCount { get; set; }

    public string? LastError { get; set; }
}
