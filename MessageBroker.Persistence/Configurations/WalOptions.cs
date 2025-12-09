namespace MessageBroker.Persistence.Configurations;

public record WalOptions
{
    public string Directory { get; init; } = Path.Combine(AppContext.BaseDirectory, "wal");
    public bool ResetOnStartup { get; set; } = false;
    public int MaxWriteCountPerFile { get; set; } = 1000;

    public FileNamingOptions FileNaming { get; init; } = new();
    public ManifestOptions Manifest { get; init; } = new();
}