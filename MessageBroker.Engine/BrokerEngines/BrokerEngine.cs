using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Exceptions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.BrokerEngines.Exceptions;
using MessageBroker.Engine.Common.Exceptions;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.BrokerEngines;

public class BrokerEngine : IBrokerEngine
{
    private readonly Lock _publishLocker = new();
    
    private readonly IMessageQueue _messageQueue;
    private readonly IWriteAheadLog _wal;
    private readonly TimeProvider _timeProvider;
    private readonly int _maxPayloadLength;
    private readonly int _maxDeliveryAttempts;

    private readonly ILogger<BrokerEngine>? _logger;

    public BrokerEngine(
        IMessageQueue? messageQueue,
        IWriteAheadLog? wal,
        TimeProvider? timeProvider,
        int maxPayloadLength,
        int maxDeliveryAttempts,
        ILogger<BrokerEngine>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(messageQueue);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(timeProvider);

        if (maxPayloadLength < 0)
        {
            throw new MaxPayloadLengthNegativeException();
        }

        if (maxDeliveryAttempts < 1)
        {
            throw new MaxDeliveryAttemptsInvalidException();
        }
        
        _messageQueue = messageQueue;
        _wal = wal;
        _timeProvider = timeProvider;
        _maxPayloadLength = maxPayloadLength;
        _maxDeliveryAttempts = maxDeliveryAttempts;
        _logger = logger;
    }
    
    public void Publish(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);

        int payloadSize = payload.Length;
        if (payloadSize > _maxPayloadLength)
        {
            throw new PayloadTooLargeException(payloadSize, _maxPayloadLength);
        }

        Message message = Message.Create(
            Guid.CreateVersion7(),
            payload,
            _maxDeliveryAttempts,
            _timeProvider);

        EnqueueWalEvent enqueueEvent = new(message.Id, message.Payload);

        bool enqueueSuccess;
        
        lock (_publishLocker)
        {
            bool walSuccess = _wal.Append(enqueueEvent);

            if (!walSuccess)
            {
                throw new BrokerStorageException();
            }
            
            enqueueSuccess = _messageQueue.TryEnqueue(message);
        }
        
        if (!enqueueSuccess)
        {
            DeadWalEvent deadEvent = new(message.Id);
            
            _wal.Append(deadEvent);

            throw new MessageQueueEnqueueException();
        }
    }

    public Message? Consume()
    {
        throw new NotImplementedException();
    }

    public void Ack(Guid messageId)
    {
        throw new NotImplementedException();
    }
}