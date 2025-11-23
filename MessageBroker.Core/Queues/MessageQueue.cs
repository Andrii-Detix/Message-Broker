using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Core.Queues.Exceptions;

namespace MessageBroker.Core.Queues;

public class MessageQueue : IMessageQueue
{
    private readonly Lock _activeMessagesLocker = new();
    private readonly Lock _addLocker = new();
    private readonly Lock _consumeLocker = new();

    private readonly HashSet<Guid> _activeMessages = [];
    private readonly ConcurrentQueue<Message> _addQueue = [];
    private readonly ConcurrentQueue<Message> _consumeQueue = [];
    private readonly ConcurrentDictionary<Guid, Message> _inFlight = [];
    private readonly int _maxSwapCount;

    private int _count = 0;

    public MessageQueue(int maxSwapCount)
    {
        if (maxSwapCount < 1)
        {
            throw new MaxSwapCountInvalidException();
        }

        _maxSwapCount = maxSwapCount;
    }

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
        lock (_consumeLocker)
        {
            if (_consumeQueue.IsEmpty && !_addQueue.IsEmpty)
            {
                SwapMessages();
            }

            if (!_consumeQueue.TryDequeue(out message))
            {
                message = null;
                return false;
            }
        }

        message.TrySend();
        _inFlight.TryAdd(message.Id, message);

        Interlocked.Decrement(ref _count);

        return true;
    }

    public Message? Ack(Guid messageId)
    {
        if (!_inFlight.TryRemove(messageId, out Message? message))
        {
            return null;
        }

        RemoveFromActiveMessages(messageId);

        message.TryMarkDelivered();
        
        return message;
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

    private void SwapMessages()
    {
        int count = 0;
        while (count < _maxSwapCount && _addQueue.TryDequeue(out var added))
        {
            _consumeQueue.Enqueue(added);
            count++;
        }
    }
}