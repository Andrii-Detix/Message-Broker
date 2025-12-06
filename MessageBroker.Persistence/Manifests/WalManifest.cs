namespace MessageBroker.Persistence.Manifests;

public record WalManifest
{
    public string Enqueue { get; init; } = string.Empty;
    
    public string Ack { get; init; } = string.Empty;
    
    public string Dead { get; init; } = string.Empty;
    
    public string Merged { get; init; } = string.Empty;
}