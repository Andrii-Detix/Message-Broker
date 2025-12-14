using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.EndToEndTests.Tests.HelperServices;

public class BrokenEnqueueFileAppender : IFileAppender<EnqueueWalEvent>
{
    public void Append(EnqueueWalEvent evt)
    {
        throw new WalStorageException("Custom exception");
    }

    public string CurrentFile => string.Empty;
    
    public void Dispose()
    {
        // Ignore
    }
}