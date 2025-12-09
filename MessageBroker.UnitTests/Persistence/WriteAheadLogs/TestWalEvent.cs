using MessageBroker.Persistence.Events;

namespace MessageBroker.UnitTests.Persistence.WriteAheadLogs;

public record TestWalEvent : WalEvent
{
    public override WalEventType Type => WalEventType.Enqueue;
}