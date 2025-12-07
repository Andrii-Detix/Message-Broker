using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.RequeueServices;

public class RequeueService : IRequeueService
{
    private readonly IMessageQueue _messageQueue;
    private readonly IWriteAheadLog _wal;
    private readonly IExpiredMessagePolicy _expiredMessagePolicy;
    private readonly ILogger<RequeueService>? _logger;

    public RequeueService(
        IMessageQueue? messageQueue,
        IWriteAheadLog? wal,
        IExpiredMessagePolicy? expiredMessagePolicy,
        ILogger<RequeueService>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(messageQueue);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(expiredMessagePolicy);
        
        _messageQueue = messageQueue;
        _wal = wal;
        _expiredMessagePolicy = expiredMessagePolicy;
        _logger = logger;
    }
    
    public void Requeue()
    {
        IEnumerable<Message> messages = _messageQueue.TakeExpiredMessages(_expiredMessagePolicy);

        foreach (var message in messages)
        {
            RequeueWalEvent requeueEvent = new(message.Id);

            _wal.Append(requeueEvent);

            bool requeueSuccess = _messageQueue.TryEnqueue(message);

            if (!requeueSuccess)
            {
                DeadWalEvent deadEvent = new(message.Id);
                
                _wal.Append(deadEvent);
            }
        }
    }
}