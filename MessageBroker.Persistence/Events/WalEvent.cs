namespace MessageBroker.Persistence.Events;

public abstract record WalEvent
{
    public abstract WalEventType Type { get; }
}