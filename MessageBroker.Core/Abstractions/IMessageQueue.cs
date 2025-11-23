using MessageBroker.Core.Messages.Models;

namespace MessageBroker.Core.Abstractions;

public interface IMessageQueue
{
    int Count { get; }

    bool TryEnqueue(Message message);

    bool TryConsume(out Message? message);
    
    Message? Ack(Guid messageId);
    
    IEnumerable<Message> TakeExpiredMessages(IExpiredMessagePolicy policy);
}