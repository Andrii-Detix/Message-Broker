namespace MessageBroker.Core.Configurations;

public record ExpiredPolicyOptions
{
    public TimeSpan ExpirationTime { get; init; } = TimeSpan.FromSeconds(10);
}