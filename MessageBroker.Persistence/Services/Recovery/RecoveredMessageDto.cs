namespace MessageBroker.Persistence.Services.Recovery;

public record RecoveredMessageDto(Guid MessageId, byte[] Payload)
{
    public Guid MessageId { get; } = MessageId;

    public byte[] Payload { get; } = Payload;

    public int DeliveryAttempts { get; set; } = 0;
}