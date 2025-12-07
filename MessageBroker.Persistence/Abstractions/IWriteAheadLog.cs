using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.Abstractions;

public interface IWriteAheadLog
{
    void Append(WalEvent evt);
}