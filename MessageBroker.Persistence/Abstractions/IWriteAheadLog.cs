using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.Abstractions;

public interface IWriteAheadLog
{
    bool Append(WalEvent evt);
}