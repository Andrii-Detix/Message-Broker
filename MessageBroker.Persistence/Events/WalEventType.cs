namespace MessageBroker.Persistence.Events;

public enum WalEventType
{
    Enqueue,
    Ack,
    Dead
}