using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public record FaultyTestWalEvent() : WalEvent(WalEventType.Enqueue);

public class CustomException() : Exception("Custom Exception.");

public class FaultyTestWalReader : AbstractWalReader<FaultyTestWalEvent>
{
    protected override bool TryReadNext(BinaryReader reader, out FaultyTestWalEvent? evt)
    {
        throw new CustomException();
    }
}