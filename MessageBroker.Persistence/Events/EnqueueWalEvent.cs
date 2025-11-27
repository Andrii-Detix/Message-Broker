namespace MessageBroker.Persistence.Events;

public record EnqueueWalEvent(Guid MessageId, byte[] Payload) 
    : WalEvent(WalEventType.Enqueue);