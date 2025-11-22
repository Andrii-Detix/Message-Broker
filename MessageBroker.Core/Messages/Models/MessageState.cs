namespace MessageBroker.Core.Messages.Models;

public enum MessageState
{
    Created,
    Enqueued,
    Sent,
    Delivered,
    Failed,
}