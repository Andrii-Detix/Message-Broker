namespace MessageBroker.Persistence.Configurations;

public record GarbageCollectorOptions
{
    public TimeSpan CollectInterval { get; init; } = TimeSpan.FromMinutes(5);
}