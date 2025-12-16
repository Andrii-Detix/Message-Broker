using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.GarbageCollector;

public class TestFileAppender<TEvent>
    : IFileAppender<TEvent>
    where TEvent : WalEvent
{
    private Exception? _exception = null;
    
    public TestFileAppender(string directory, string fileName)
    {
        CurrentFile = Path.Combine(directory, fileName);
    }
    
    public string CurrentFile { get; }
    
    public int AppendInvocationCount { get; private set; } = 0;
    
    public List<TEvent> Events { get; } = [];
    
    public void Append(TEvent evt)
    {
        if (!File.Exists(CurrentFile))
        {
            File.Create(CurrentFile).Dispose();
        }
        
        if (_exception is not null)
        {
            throw _exception;
        }

        Events.Add(evt);
        AppendInvocationCount++;
    }

    public void Verify(Func<TEvent, bool> func, int times)
    {
        int matchCount = Events.Count(func);
        matchCount.ShouldBe(times);
    }
    
    public void SetExceptionOnAppend(Exception exception)
    {
        _exception = exception;
    }
    
    public void Dispose()
    { 
        // Ignore
    }
}