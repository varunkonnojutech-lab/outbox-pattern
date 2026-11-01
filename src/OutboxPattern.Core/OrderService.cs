using System.Text.Json;

namespace OutboxPattern.Core;

/// <summary>
/// Places orders. The key line is the single <c>SaveChangesAsync</c>: EF Core
/// wraps it in one database transaction, so the order row and its outbox
/// row commit together or not at all. That is the whole trick — there is no
/// separate "publish" step here to fail independently of the save.
/// </summary>
public sealed class OrderService
{
    private readonly AppDbContext _db;

    public OrderService(AppDbContext db) => _db = db;

    public async Task<Order> PlaceOrderAsync(
        string customerEmail, decimal totalAmount, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(customerEmail))
            throw new ArgumentException("Customer email is required.", nameof(customerEmail));
        if (totalAmount <= 0)
            throw new ArgumentException("Total must be positive.", nameof(totalAmount));

        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerEmail = customerEmail,
            TotalAmount = totalAmount,
        };

        var @event = new OutboxMessage
        {
            Type = "OrderPlaced",
            Payload = JsonSerializer.Serialize(new
            {
                orderId = order.Id,
                customerEmail = order.CustomerEmail,
                totalAmount = order.TotalAmount,
            }),
        };

        _db.Orders.Add(order);
        _db.OutboxMessages.Add(@event);

        // One SaveChanges = one transaction = order + event, atomically.
        await _db.SaveChangesAsync(ct);
        return order;
    }
}
