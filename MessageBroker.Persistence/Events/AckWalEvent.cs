namespace MessageBroker.Persistence.Events;

public record AckWalEvent(Guid MessageId)
    : WalEvent(WalEventType.Ack);