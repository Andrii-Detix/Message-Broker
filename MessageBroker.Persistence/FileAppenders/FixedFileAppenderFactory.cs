using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FilePathCreators;

namespace MessageBroker.Persistence.FileAppenders;

public class FixedFileAppenderFactory : IFileAppenderFactory
{
    private const int MaxWritesPerFile = int.MaxValue;
    
    private readonly ICrcProvider _crcProvider;
    private readonly IFilePathCreator _enqueueDynamicFilePathCreator;
    private readonly IFilePathCreator _ackDynamicFilePathCreator;
    private readonly IFilePathCreator _deadDynamicFilePathCreator;

    public FixedFileAppenderFactory(
        ICrcProvider crcProvider,
        IFilePathCreator enqueueDynamicFilePathCreator,
        IFilePathCreator ackDynamicFilePathCreator,
        IFilePathCreator deadDynamicFilePathCreator)
    {
        ArgumentNullException.ThrowIfNull(crcProvider);
        ArgumentNullException.ThrowIfNull(enqueueDynamicFilePathCreator);
        ArgumentNullException.ThrowIfNull(ackDynamicFilePathCreator);
        ArgumentNullException.ThrowIfNull(deadDynamicFilePathCreator);
        
        _crcProvider = crcProvider;
        _enqueueDynamicFilePathCreator = enqueueDynamicFilePathCreator;
        _ackDynamicFilePathCreator = ackDynamicFilePathCreator;
        _deadDynamicFilePathCreator = deadDynamicFilePathCreator;
    }
    
    public IFileAppender<TEvent> Create<TEvent>() 
        where TEvent : WalEvent
    {
        string path = CreatePath<TEvent>();
        
        IFilePathCreator pathCreator = new FixedFilePathCreator(path);
        
        return CreateAppender<TEvent>(pathCreator);
    }

    private string CreatePath<TEvent>()
        where TEvent : WalEvent
    {
        return typeof(TEvent) switch
        {
            _ when typeof(TEvent) == typeof(EnqueueWalEvent) => 
                _enqueueDynamicFilePathCreator.CreatePath(),
            
            _ when typeof(TEvent) == typeof(AckWalEvent) =>
                _ackDynamicFilePathCreator.CreatePath(),
            
            _ when typeof(TEvent) == typeof(DeadWalEvent) =>
                _deadDynamicFilePathCreator.CreatePath(),
            
            _ => throw new UnknownWalEventTypeException()
        };
    }

    private IFileAppender<TEvent> CreateAppender<TEvent>(IFilePathCreator filePathCreator)
        where TEvent : WalEvent
    {
        object appender = typeof(TEvent) switch
        {
            _ when typeof(TEvent) == typeof(EnqueueWalEvent) => 
                new EnqueueFileAppender(_crcProvider, filePathCreator, MaxWritesPerFile),
                
            _ when typeof(TEvent) == typeof(AckWalEvent) => 
                new AckFileAppender(_crcProvider, filePathCreator, MaxWritesPerFile),
                
            _ when typeof(TEvent) == typeof(DeadWalEvent) => 
                new DeadFileAppender(_crcProvider, filePathCreator, MaxWritesPerFile),
                
            _ => throw new UnknownWalEventTypeException()
        };
        
        return (IFileAppender<TEvent>)appender;
    }
}