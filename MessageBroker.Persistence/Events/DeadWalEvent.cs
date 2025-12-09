namespace MessageBroker.Persistence.Events;

public record DeadWalEvent(Guid MessageId) : WalEvent
{
    public override WalEventType Type => WalEventType.Dead;
}