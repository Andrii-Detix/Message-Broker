using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public record FaultyTestWalEvent() : WalEvent(WalEventType.Enqueue);

public class CustomException() : Exception("Custom Exception.");

public class FaultyTestWalReader(ICrcProvider crcProvider) 
    : AbstractWalReader<FaultyTestWalEvent>(crcProvider)
{
    protected override FaultyTestWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        throw new CustomException();
    }
}