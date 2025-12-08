using MessageBroker.Persistence.Configurations;

namespace MessageBroker.Engine.Configurations;

public record MessageBrokerOptions
{
    public BrokerOptions Broker { get; init; } = new();
    public WalOptions Wal { get; init; } = new();
}