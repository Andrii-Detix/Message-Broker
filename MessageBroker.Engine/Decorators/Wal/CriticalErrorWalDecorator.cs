using MessageBroker.Engine.Abstractions;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Engine.Decorators.Wal;

public class CriticalErrorWalDecorator(
    IWriteAheadLog innerWal,
    ICriticalErrorService criticalErrorService)
    : IWriteAheadLog
{
    private readonly Lock _locker = new();

    private volatile bool _isHealthy = true;

    public void Append(WalEvent evt)
    {
        if (!_isHealthy)
        {
            throw new WalStorageException("Wal storage can't process events.");
        }

        try
        {
            innerWal.Append(evt);
        }
        catch (WalStorageException ex)
        {
            HandleCriticalError(ex);
            throw;
        }
    }

    private void HandleCriticalError(Exception ex)
    {
        if (!_isHealthy)
        {
            return;
        }

        lock (_locker)
        {
            if (!_isHealthy)
            {
                return;
            }

            _isHealthy = false;
            criticalErrorService.Raise(
                $"Persistence layer reported critical failure with error '{ex.Message}'",
                ex);
        }
    }
}