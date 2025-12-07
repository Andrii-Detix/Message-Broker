using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;

namespace MessageBroker.Core.Queues;

public class MessageQueueFactory : IMessageQueueFactory
{
    private readonly MessageQueueOptions _config;
    private readonly TimeProvider _timeProvider;
    
    public MessageQueueFactory(MessageQueueOptions config, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(timeProvider);
        
        _config = config;
        _timeProvider = timeProvider;
    }
    
    public IMessageQueue Create()
    {
        return new MessageQueue(_config.MaxSwapCount, _timeProvider);
    }
}