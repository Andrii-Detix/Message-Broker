namespace MessageBroker.Persistence.Events;

public record RequeueWalEvent(Guid MessageId) 
    : EnqueueWalEvent(MessageId, []);