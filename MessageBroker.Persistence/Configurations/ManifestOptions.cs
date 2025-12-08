namespace MessageBroker.Persistence.Configurations;

public record ManifestOptions
{
    public string FileName { get; init; } = "manifest.json";
}