using MessageBroker.Core.Abstractions;

namespace MessageBroker.Persistence.Abstractions;

public interface IRecoveryService
{
    IMessageQueue Recover();
}