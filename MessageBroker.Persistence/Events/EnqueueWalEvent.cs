namespace MessageBroker.Persistence.Events;

public record EnqueueWalEvent(Guid MessageId, byte[] Payload) : WalEvent
{
    public override WalEventType Type => WalEventType.Enqueue;
}