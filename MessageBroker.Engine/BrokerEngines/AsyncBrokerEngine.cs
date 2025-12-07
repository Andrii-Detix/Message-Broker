using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;

namespace MessageBroker.Engine.BrokerEngines;

public class AsyncBrokerEngine : IAsyncBrokerEngine
{
    private readonly IBrokerEngine _syncEngine;

    public AsyncBrokerEngine(IBrokerEngine syncEngine) 
    {
        ArgumentNullException.ThrowIfNull(syncEngine);
        
        _syncEngine = syncEngine;
    }
    
    public Task PublishAsync(byte[] payload)
    {
        return Task.Run(() => _syncEngine.Publish(payload));
    }

    public Task<Message?> ConsumeAsync()
    {
        return Task.Run(() => _syncEngine.Consume());
    }
    
    public Task AckAsync(Guid messageId)
    {
        return Task.Run(() => _syncEngine.Ack(messageId));
    }
}