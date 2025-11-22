using MessageBroker.Core.Messages.Exceptions;

namespace MessageBroker.Core.Messages.Models;

public class Message
{
    private Message(
        Guid id,
        byte[] payload,
        MessageState state,
        DateTimeOffset createdAt,
        DateTimeOffset? lastSentAt,
        int deliveryCount,
        int maxDeliveryAttempts)
    {
        Id = id;
        Payload = payload;
        State = state;
        CreatedAt = createdAt;
        LastSentAt = lastSentAt;
        DeliveryCount = deliveryCount;
        MaxDeliveryAttempts = maxDeliveryAttempts;
    }

    public Guid Id { get; }

    public byte[] Payload { get; }

    public MessageState State { get; private set; }

    public DateTimeOffset CreatedAt { get; }

    public DateTimeOffset? LastSentAt { get; private set; }

    public int DeliveryCount { get; private set; }

    public int MaxDeliveryAttempts { get; }

    public static Message Create(Guid id, byte[] payload, int maxDeliveryAttempts)
    {
        if (payload is null)
        {
            throw new PayloadNullReferenceException();
        }

        if (maxDeliveryAttempts < 1)
        {
            throw new MaxDeliveryAttemptsInvalidException();
        }

        return new(
            id,
            payload,
            MessageState.Created,
            DateTimeOffset.UtcNow,
            null,
            0,
            maxDeliveryAttempts);
    }

    public bool TryEnqueue()
    {
        if (State != MessageState.Created && State != MessageState.Sent)
        {
            return false;
        }

        if (DeliveryCount >= MaxDeliveryAttempts)
        {
            Fail();
            return false;
        }

        State = MessageState.Enqueued;

        return true;
    }

    public bool TrySend()
    {
        if (State != MessageState.Enqueued)
        {
            return false;
        }

        State = MessageState.Sent;
        LastSentAt = DateTimeOffset.UtcNow;
        DeliveryCount++;

        return true;
    }

    public bool TryMarkDelivered()
    {
        if (State != MessageState.Sent)
        {
            return false;
        }

        State = MessageState.Delivered;

        return true;
    }

    private void Fail()
    {
        State = MessageState.Failed;
    }
}