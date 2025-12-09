using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;

namespace MessageBroker.IntegrationTests.Persistence.Recovery;

public class RecoveryTestMessageQueue : IMessageQueue
{
    private readonly Queue<Message> _queue = [];
    
    public int Count => _queue.Count;
    
    public bool TryEnqueue(Message message)
    {
        _queue.Enqueue(message);
        
        return true;
    }

    public bool TryConsume(out Message? message)
    {
        return _queue.TryDequeue(out message);
    }

    public Message? Ack(Guid messageId)
    {
        return null;
    }

    public IEnumerable<Message> TakeExpiredMessages(IExpiredMessagePolicy policy)
    {
        return [];
    }
}