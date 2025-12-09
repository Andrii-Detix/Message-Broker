namespace MessageBroker.Persistence.Manifests;

public record WalManifest
{
    public string Enqueue { get; init; } = string.Empty;
    
    public string Ack { get; init; } = string.Empty;
    
    public string Dead { get; init; } = string.Empty;
    
    public string EnqueueMerged { get; init; } = string.Empty;
    
    public string AckMerged { get; init; } = string.Empty;
    
    public string DeadMerged { get; init; } = string.Empty;
}