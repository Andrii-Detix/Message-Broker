using MessageBroker.Core.Messages.Models;

namespace MessageBroker.Core.Abstractions;

public interface IExpiredMessagePolicy
{
    bool IsExpired(Message message);
}