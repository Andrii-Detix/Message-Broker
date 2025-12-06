using System.Buffers.Binary;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public record TestWalEvent(int Value) : WalEvent(WalEventType.Enqueue);

public class TestWalReader(ICrcProvider crcProvider) 
    : AbstractWalReader<TestWalEvent>(crcProvider)
{
    protected override TestWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        int value = BinaryPrimitives.ReadInt32LittleEndian(data);

        TestWalEvent evt = new(value);
        
        return evt;
    }
}