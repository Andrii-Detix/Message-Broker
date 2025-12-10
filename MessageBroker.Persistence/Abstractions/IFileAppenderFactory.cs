using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.Abstractions;

public interface IFileAppenderFactory
{
    IFileAppender<TEvent> Create<TEvent>() where TEvent : WalEvent;
}