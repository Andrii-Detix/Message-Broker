using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public record TestWalEvent(int Value) : WalEvent(WalEventType.Enqueue);

public class TestWalReader : AbstractWalReader<TestWalEvent>
{
    protected override bool TryReadNext(BinaryReader reader, out TestWalEvent? evt)
    {
        if (!CanRead(reader.BaseStream, 4))
        {
            evt = null;
            return false;
        }
        
        int data = reader.ReadInt32();
        
        evt = new TestWalEvent(data);
        
        return true;
    }
}