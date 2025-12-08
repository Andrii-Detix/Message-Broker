namespace MessageBroker.Core.Configurations;

public record MessageOptions
{
    public int MaxDeliveryAttempts { get; init; } = 3;
    public int MaxPayloadSize { get; init; } = 1024 * 1024;
}