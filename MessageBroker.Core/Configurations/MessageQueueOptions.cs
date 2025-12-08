namespace MessageBroker.Core.Configurations;

public record MessageQueueOptions
{
    public int MaxSwapCount { get; init; } = 5;
}