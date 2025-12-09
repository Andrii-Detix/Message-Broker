namespace MessageBroker.Persistence.Configurations;

public record FileNamingOptions
{
    public string Extension { get; init; } = "log";
    
    public string EnqueuePrefix { get; init; } = "enqueue";
    public string AckPrefix { get; init; } = "ack";
    public string DeadPrefix { get; init; } = "dead";
    
    public string EnqueueMergedPrefix { get; init; } = "enqueue-merged";
    public string AckMergedPrefix { get; init; } = "ack-merged";
    public string DeadMergedPrefix { get; init; } = "dead-merged";
}