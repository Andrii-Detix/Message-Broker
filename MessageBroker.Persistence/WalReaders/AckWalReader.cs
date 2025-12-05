using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class AckWalReader : AbstractWalReader<AckWalEvent>
{
    protected override bool TryReadNext(BinaryReader reader, out AckWalEvent? evt)
    {
        const int guidLength = 16;
        
        evt = null;
        
        if (!CanRead(reader.BaseStream, guidLength))
        {
            return false;
        }

        byte[] buffer = reader.ReadBytes(guidLength);
        Guid messageId = new Guid(buffer);
        
        evt = new AckWalEvent(messageId);
        
        return true;
    }
}