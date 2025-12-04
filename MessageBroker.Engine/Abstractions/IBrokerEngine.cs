using MessageBroker.Core.Messages.Models;

namespace MessageBroker.Engine.Abstractions;

public interface IBrokerEngine
{
    void Publish(byte[] payload);

    Message? Consume();

    void Ack(Guid messageId);
}