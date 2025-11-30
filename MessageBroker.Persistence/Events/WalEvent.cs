namespace MessageBroker.Persistence.Events;

public abstract record WalEvent(WalEventType EventType);