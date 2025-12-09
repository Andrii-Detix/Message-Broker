namespace MessageBroker.Persistence.Events;

public record RequeueWalEvent(Guid MessageId)
    : EnqueueWalEvent(MessageId, [])
{
    public override WalEventType Type => WalEventType.Requeue;
}