namespace OutboxPattern.Core;

/// <summary>
/// A customer order. In a real system this would carry lines, money, and a
/// lifecycle; here it is deliberately small so the outbox mechanics stay
/// in focus.
/// </summary>
public sealed class Order
{
    public Guid Id { get; set; }
    public string CustomerEmail { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public string Status { get; set; } = "Placed";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
