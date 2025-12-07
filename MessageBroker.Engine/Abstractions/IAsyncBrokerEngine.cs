using MessageBroker.Core.Messages.Models;

namespace MessageBroker.Engine.Abstractions;

public interface IAsyncBrokerEngine
{
    Task PublishAsync(byte[] payload);
    
    Task<Message?> ConsumeAsync();
    
    Task AckAsync(Guid messageId);
}