using MessageBroker.Persistence.Events;

namespace MessageBroker.IntegrationTests.Persistence.WalStorage;

public record TestWalEvent(Guid MessageId) : WalEvent
{
    public override WalEventType Type => WalEventType.Enqueue;
}