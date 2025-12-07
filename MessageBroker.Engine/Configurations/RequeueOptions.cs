namespace MessageBroker.Engine.Configurations;

public record RequeueOptions
{
    public TimeSpan RequeueInterval { get; init; } = TimeSpan.FromSeconds(10);
}