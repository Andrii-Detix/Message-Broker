using MessageBroker.Core.Configurations;

namespace MessageBroker.Engine.Configurations;

public record BrokerOptions
{
    public MessageOptions Message { get; init; } = new();
    public MessageQueueOptions Queue { get; init; } = new();
    public ExpiredPolicyOptions ExpiredPolicy { get; init; } = new();
    public RequeueOptions Requeue { get; init; } = new();
}