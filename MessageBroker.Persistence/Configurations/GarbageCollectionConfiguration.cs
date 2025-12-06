namespace MessageBroker.Persistence.Configurations;

public record GarbageCollectionConfiguration
{
    public string FileName { get; init; } = "merged";
}