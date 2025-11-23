using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;

namespace MessageBroker.Core.Queues;

public class MessageQueue : IMessageQueue
{
    private readonly Lock _activeMessagesLocker = new();
    private readonly Lock _addLocker = new();

    private readonly HashSet<Guid> _activeMessages = [];
    private readonly ConcurrentQueue<Message> _addQueue = [];

    private int _count = 0;

    public int Count => _count;

    public bool TryEnqueue(Message message)
    {
        lock (_addLocker)
        {
            if (ExistsById(message.Id) || !message.TryEnqueue())
            {
                return false;
            }
            
            AddToActiveMessages(message.Id);
        }
        
        _addQueue.Enqueue(message);

        Interlocked.Increment(ref _count);

        return true;
   }

    public bool TryConsume(out Message? message)
    {
        throw new NotImplementedException();
    }

    public Message? Ack(Guid messageId)
    {
        throw new NotImplementedException();
    }

    public IEnumerable<Message> TakeExpiredMessages(IExpiredMessagePolicy policy)
    {
        throw new NotImplementedException();
    }

    private bool ExistsById(Guid messageId)
    {
        lock (_activeMessagesLocker)
        {
            return _activeMessages.Contains(messageId);
        }
    }

    private void AddToActiveMessages(Guid messageId)
    {
        lock (_activeMessagesLocker)
        {
            _activeMessages.Add(messageId);
        }
    }

    private void RemoveFromActiveMessages(Guid messageId)
    {
        lock (_activeMessagesLocker)
        {
            _activeMessages.Remove(messageId);
        }
    }
}