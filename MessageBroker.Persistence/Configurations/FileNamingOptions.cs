namespace MessageBroker.Persistence.Configurations;

public record FileNamingOptions
{
    public string Extension { get; init; } = "log";
    
    public string EnqueuePrefix { get; init; } = "enqueue";
    public string AckPrefix { get; init; } = "ack";
    public string DeadPrefix { get; init; } = "dead";
    
    public string MergePrefix { get; init; } = "merge";
}