using Microsoft.EntityFrameworkCore;
using OutboxPattern.Core;

// A SQLite file in the working directory (gitignored via *.db). No Docker,
// no broker, no external services: the "bus" is in-memory.

var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseSqlite("Data Source=outbox.db")
    .Options;

await using var db = new AppDbContext(options);
await db.Database.EnsureDeletedAsync();
await db.Database.EnsureCreatedAsync();

var orders = new OrderService(db);
var bus = new InMemoryMessageBus();
var flakyBus = new FlakyMessageBus(bus, failures: 1);
var relay = new OutboxRelay(db, flakyBus);

Console.WriteLine("1. Place two orders. Each save writes the order AND its");
Console.WriteLine("   outbox row in one transaction — no dual write, nothing lost.");
await orders.PlaceOrderAsync("ada@example.com", 49.99m);
await orders.PlaceOrderAsync("grace@example.com", 129.50m);
Console.WriteLine($"   Orders: {await db.Orders.CountAsync()}, " +
                  $"unpublished outbox rows: {await db.OutboxMessages.CountAsync(m => m.PublishedAt == null)}");

Console.WriteLine();
Console.WriteLine("2. Relay pass 1: the broker is 'down' for the first message.");
Console.WriteLine($"   Published this pass: {await relay.ProcessPendingAsync()}");
Console.WriteLine($"   Delivered: {bus.Delivered.Count}, " +
                  $"still unpublished: {await db.OutboxMessages.CountAsync(m => m.PublishedAt == null)}");

Console.WriteLine();
Console.WriteLine("3. Relay pass 2: the broker is back. Nothing was lost —");
Console.WriteLine("   the failed message is retried and everything publishes.");
Console.WriteLine($"   Published this pass: {await relay.ProcessPendingAsync()}");
Console.WriteLine($"   Delivered: {bus.Delivered.Count}, " +
                  $"still unpublished: {await db.OutboxMessages.CountAsync(m => m.PublishedAt == null)}");

Console.WriteLine();
Console.WriteLine("4. Relay pass 3: already-published rows are never re-sent.");
Console.WriteLine($"   Published this pass: {await relay.ProcessPendingAsync()}");
Console.WriteLine($"   Delivered total (still {bus.Delivered.Count} — no duplicates):");
foreach (var msg in bus.Delivered)
    Console.WriteLine($"   - {msg.Type}: {msg.Payload}");
