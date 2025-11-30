using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.Abstractions;

public interface IFileAppender<in TEvent>
    where TEvent : WalEvent
{
    void Append(TEvent evt);
    
    string CurrentFile { get; }
}