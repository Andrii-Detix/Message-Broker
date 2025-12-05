using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class DeadWalReader : AbstractWalReader<DeadWalEvent>
{
    protected override bool TryReadNext(BinaryReader reader, out DeadWalEvent? evt)
    {
        const int guidLength = 16;
        
        evt = null;
        
        if (!CanRead(reader.BaseStream, guidLength))
        {
            return false;
        }

        byte[] buffer = reader.ReadBytes(guidLength);
        Guid messageId = new Guid(buffer);
        
        evt = new DeadWalEvent(messageId);
        
        return true;
    }
}