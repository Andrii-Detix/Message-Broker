using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class EnqueueWalReader : AbstractWalReader<EnqueueWalEvent>
{
    protected override bool TryReadNext(BinaryReader reader, out EnqueueWalEvent? evt)
    {
        const int intSize = 4;
        const int guidSize = 16;
        
        evt = null;
        
        // Read stored event length
        if (!CanRead(reader.BaseStream, intSize))
        {
            return false;
        }

        int length = reader.ReadInt32();
        int restLength = length - intSize;

        // Read rest of stored event
        if (!CanRead(reader.BaseStream, restLength))
        {
            return false;
        }

        WalEventType type = (WalEventType)reader.ReadInt32();

        byte[] buffer = reader.ReadBytes(guidSize);
        Guid messageId = new Guid(buffer);

        int payloadLength = restLength - intSize - guidSize;
        buffer = reader.ReadBytes(payloadLength);

        evt = new EnqueueWalEvent(messageId, buffer);
        
        return true;
    }
}