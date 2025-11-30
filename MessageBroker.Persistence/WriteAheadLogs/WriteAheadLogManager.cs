using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WriteAheadLogs.Exceptions;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Persistence.WriteAheadLogs;

public class WriteAheadLogManager : IWriteAheadLog
{
    private readonly IFileAppender<EnqueueWalEvent> _enqueueAppender;
    private readonly IFileAppender<AckWalEvent> _ackAppender;
    private readonly IFileAppender<DeadWalEvent> _deadAppender;

    private readonly ILogger<WriteAheadLogManager>? _logger;

    public WriteAheadLogManager(
        IFileAppender<EnqueueWalEvent>? enqueueAppender,
        IFileAppender<AckWalEvent>? ackAppender,
        IFileAppender<DeadWalEvent>? deadAppender,
        ILogger<WriteAheadLogManager>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(enqueueAppender);
        ArgumentNullException.ThrowIfNull(ackAppender);
        ArgumentNullException.ThrowIfNull(deadAppender);
        
        _enqueueAppender = enqueueAppender;
        _ackAppender = ackAppender;
        _deadAppender = deadAppender;
        _logger = logger;
    }
    
    public bool Append(WalEvent evt)
    {
        try
        {
            DispatchWalEvent(evt);
            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, $"Storage of event '{evt.GetType().Name}' completed with failure '{ex.Message}'.");
            
            return false;
        }
    }

    private void DispatchWalEvent(WalEvent evt)
    {
        switch (evt)
        {
            case EnqueueWalEvent e:
                _enqueueAppender.Append(e);
                break;
            
            case AckWalEvent a:
                _ackAppender.Append(a);
                break;
            
            case DeadWalEvent d:
                _deadAppender.Append(d);
                break;
            
            default:
                throw new WalEventUnknownTypeException();
        }
    }
}