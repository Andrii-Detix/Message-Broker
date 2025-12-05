namespace MessageBroker.Core.Messages.Models;

public enum MessageState
{
    Created,
    Restored,
    Enqueued,
    Sent,
    Delivered,
    Failed,
}