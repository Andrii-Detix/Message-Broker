namespace MessageBroker.Persistence.Configurations;

public record WalFileBaseNames
{
    public string Enqueue { get; init; } = "enqueue";
    public string Ack { get; init; } = "ack";
    public string Dead { get; init; } = "dead";
}