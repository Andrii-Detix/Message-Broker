namespace MessageBroker.Persistence.Events;

public record AckWalEvent(Guid MessageId) : WalEvent
{
    public override WalEventType Type => WalEventType.Ack;
}