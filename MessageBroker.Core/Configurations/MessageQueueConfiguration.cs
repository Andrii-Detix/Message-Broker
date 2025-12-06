namespace MessageBroker.Core.Configurations;

public record MessageQueueConfiguration
{
    public int MaxSwapCount { get; init; } = 5;
}