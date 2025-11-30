namespace MessageBroker.Persistence.Events;

public record DeadWalEvent(Guid MessageId)
    : WalEvent(WalEventType.Dead);