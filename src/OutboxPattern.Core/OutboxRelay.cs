using Microsoft.EntityFrameworkCore;

namespace OutboxPattern.Core;

/// <summary>
/// The relay: a background worker that polls for unpublished outbox rows,
/// publishes them, and only then marks them published. Ordering is by
/// creation time (oldest first); batching keeps each poll bounded.
/// A crash between publish and marking published is safe: the message is
/// simply published again on the next pass, which is why delivery is
/// at-least-once and consumers must be idempotent.
/// </summary>
public sealed class OutboxRelay
{
    private readonly AppDbContext _db;
    private readonly IMessageBus _bus;

    public OutboxRelay(AppDbContext db, IMessageBus bus)
    {
        _db = db;
        _bus = bus;
    }

    /// <returns>Number of messages successfully published in this pass.</returns>
    public async Task<int> ProcessPendingAsync(int batchSize = 20, CancellationToken ct = default)
    {
        var pending = await _db.OutboxMessages
            .Where(m => m.PublishedAt == null)
            .OrderBy(m => m.CreatedAt)
            .Take(batchSize)
            .ToListAsync(ct);

        var published = 0;
        foreach (var message in pending)
        {
            try
            {
                await _bus.PublishAsync(message, ct);
                message.PublishedAt = DateTime.UtcNow;
                message.LastError = null;
                published++;
            }
            catch (Exception ex)
            {
                // Do NOT mark published. The row stays pending and the next
                // pass retries it; RetryCount tells operators something is stuck.
                message.RetryCount++;
                message.LastError = ex.Message;
            }

            // Save per message so one failure cannot roll back other publishes.
            await _db.SaveChangesAsync(ct);
        }

        return published;
    }
}
