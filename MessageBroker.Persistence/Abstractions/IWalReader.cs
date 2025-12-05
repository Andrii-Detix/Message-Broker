using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.Abstractions;

public interface IWalReader<out TEvent>
    where TEvent : WalEvent
{
    IEnumerable<TEvent> Read(string filePath);
}