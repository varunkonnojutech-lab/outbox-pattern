using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OutboxPattern.Core;

namespace OutboxPattern.Tests;

/// <summary>
/// Each test gets a fresh in-memory SQLite database. The connection is kept
/// open for the test's lifetime because an in-memory SQLite database only
/// lives as long as its connection does.
/// </summary>
public sealed class OutboxTests : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly AppDbContext _db;

    public OutboxTests()
    {
        _connection = new SqliteConnection("Data Source=:memory:");
        _connection.Open();
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlite(_connection)
            .Options;
        _db = new AppDbContext(options);
        _db.Database.EnsureCreated();
    }

    public void Dispose()
    {
        _db.Dispose();
        _connection.Dispose();
    }

    [Fact]
    public async Task PlaceOrder_WritesOrderAndOutboxRowTogether()
    {
        var orders = new OrderService(_db);

        var order = await orders.PlaceOrderAsync("ada@example.com", 49.99m);

        Assert.Equal(1, await _db.Orders.CountAsync());
        var message = await _db.OutboxMessages.SingleAsync();
        Assert.Equal("OrderPlaced", message.Type);
        Assert.Contains(order.Id.ToString(), message.Payload);
        Assert.Null(message.PublishedAt); // not published yet — the relay does that
    }

    [Fact]
    public async Task PlaceOrder_WithInvalidInput_SavesNothing()
    {
        var orders = new OrderService(_db);

        await Assert.ThrowsAsync<ArgumentException>(
            () => orders.PlaceOrderAsync("ada@example.com", 0m));

        // Atomicity in the other direction too: a rejected order leaves
        // no orphan outbox row behind.
        Assert.Equal(0, await _db.Orders.CountAsync());
        Assert.Equal(0, await _db.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Relay_PublishesPendingAndMarksThemPublished()
    {
        var orders = new OrderService(_db);
        await orders.PlaceOrderAsync("ada@example.com", 49.99m);
        await orders.PlaceOrderAsync("grace@example.com", 129.50m);
        var bus = new InMemoryMessageBus();
        var relay = new OutboxRelay(_db, bus);

        var published = await relay.ProcessPendingAsync();

        Assert.Equal(2, published);
        Assert.Equal(2, bus.Delivered.Count);
        Assert.Equal(0, await _db.OutboxMessages.CountAsync(m => m.PublishedAt == null));
    }

    [Fact]
    public async Task Relay_DoesNotRepublishAlreadyPublishedMessages()
    {
        var orders = new OrderService(_db);
        await orders.PlaceOrderAsync("ada@example.com", 49.99m);
        var bus = new InMemoryMessageBus();
        var relay = new OutboxRelay(_db, bus);

        await relay.ProcessPendingAsync();
        var secondPass = await relay.ProcessPendingAsync();

        Assert.Equal(0, secondPass);
        Assert.Single(bus.Delivered);
    }

    [Fact]
    public async Task Relay_FailedPublish_StaysUnpublishedForRetry()
    {
        var orders = new OrderService(_db);
        await orders.PlaceOrderAsync("ada@example.com", 49.99m);
        var bus = new InMemoryMessageBus();
        var flaky = new FlakyMessageBus(bus, failures: 1);
        var relay = new OutboxRelay(_db, flaky);

        var firstPass = await relay.ProcessPendingAsync();
        Assert.Equal(0, firstPass);
        Assert.Empty(bus.Delivered);
        var message = await _db.OutboxMessages.SingleAsync();
        Assert.Null(message.PublishedAt);
        Assert.Equal(1, message.RetryCount);
        Assert.NotNull(message.LastError);

        // The next pass retries — and succeeds. Nothing was lost.
        var secondPass = await relay.ProcessPendingAsync();
        Assert.Equal(1, secondPass);
        Assert.Single(bus.Delivered);
    }

    [Fact]
    public async Task Relay_PublishesOldestFirst()
    {
        var orders = new OrderService(_db);
        await orders.PlaceOrderAsync("first@example.com", 10m);
        await orders.PlaceOrderAsync("second@example.com", 20m);
        await orders.PlaceOrderAsync("third@example.com", 30m);
        var bus = new InMemoryMessageBus();
        var relay = new OutboxRelay(_db, bus);

        await relay.ProcessPendingAsync(batchSize: 2);

        Assert.Equal(2, bus.Delivered.Count);
        Assert.Contains("first@example.com", bus.Delivered[0].Payload);
        Assert.Contains("second@example.com", bus.Delivered[1].Payload);
    }

    [Fact]
    public async Task Bus_DeduplicatesRedeliveryByMessageId()
    {
        // Even if a relay crash causes a redelivery, consumers see each
        // event once. This is why outbox delivery is at-least-once, and
        // why consumers must still be idempotent.
        var bus = new InMemoryMessageBus();
        var message = new OutboxMessage { Type = "OrderPlaced", Payload = "{}" };

        await bus.PublishAsync(message);
        await bus.PublishAsync(message);

        Assert.Single(bus.Delivered);
        Assert.Equal(1, bus.DuplicateDeliveriesIgnored);
    }
}
