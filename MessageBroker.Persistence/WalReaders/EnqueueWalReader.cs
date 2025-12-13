using System.Buffers.Binary;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Constants;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class EnqueueWalReader(ICrcProvider crcProvider) 
    : AbstractWalReader<EnqueueWalEvent>(crcProvider)
{
    protected override EnqueueWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        int offset = 0;

        WalEventType eventType = (WalEventType)BinaryPrimitives
            .ReadInt32LittleEndian(data.Slice(offset, BinaryLayout.IntSize));
        offset += BinaryLayout.IntSize;
        
        Guid messageId = new Guid(data.Slice(offset, BinaryLayout.GuidSize));
        offset += BinaryLayout.GuidSize;

        byte[] payload = data.Slice(offset).ToArray();

        EnqueueWalEvent evt = eventType switch
        {
            WalEventType.Enqueue => new EnqueueWalEvent(messageId, payload),
            WalEventType.Requeue => new RequeueWalEvent(messageId),
            _ => throw new UnknownWalEventTypeException()
        };
        
        return evt;
    }
}