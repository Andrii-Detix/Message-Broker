namespace MessageBroker.Persistence.Events;

public enum WalEventType
{
    Enqueue,
    Requeue,
    Ack,
    Dead
}