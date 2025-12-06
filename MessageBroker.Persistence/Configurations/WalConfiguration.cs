namespace MessageBroker.Persistence.Configurations;

public record WalConfiguration
{
    public string Directory { get; init; } = "/data/wal";
    public string FileExtension { get; init; } = "log";

    public WalFileBaseNames FileBaseNames { get; init; } = new();
    public GarbageCollectionConfiguration GarbageCollection { get; init; } = new();
    public ManifestConfiguration Manifest { get; init; } = new();
}