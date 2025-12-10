namespace MessageBroker.Persistence.Abstractions;

public interface IWalGarbageCollectorService
{
    void Collect();
}