namespace MessageBroker.Core.Configurations;

public record MessageConfiguration
{
    public int MaxDeliveryAttempts { get; set; } = 3;
}