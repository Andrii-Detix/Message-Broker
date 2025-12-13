using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.WalReaders;

namespace MessageBroker.IntegrationTests.Persistence.WalStorage;

public class TestWalReader(ICrcProvider crcProvider) 
    : AbstractWalReader<TestWalEvent>(crcProvider)
{
    protected override TestWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        Guid messageId = new Guid(data);
        
        return new(messageId);
    }
}