namespace MessageBroker.Persistence.Manifests;

public record WalFiles
{
    public List<string> EnqueueFiles { get; init; } = [];
    
    public List<string> AckFiles { get; init; } = [];
    
    public List<string> DeadFiles { get; init; } = [];
    
    public string MergedFile { get; init; } = string.Empty;
}