namespace MessageBroker.Persistence.Configurations;

public record ManifestConfiguration
{
    public string FileName { get; init; } = "manifest.json";
}