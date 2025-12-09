using System.Buffers.Binary;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class EnqueueWalReader(ICrcProvider crcProvider) 
    : AbstractWalReader<EnqueueWalEvent>(crcProvider)
{
    protected override EnqueueWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        const int intSize = 4;
        const int guidSize = 16;

        int offset = 0;

        WalEventType eventType = (WalEventType)BinaryPrimitives
            .ReadInt32LittleEndian(data.Slice(offset, intSize));
        offset += intSize;
        
        Guid messageId = new Guid(data.Slice(offset, guidSize));
        offset += guidSize;

        int payloadLength = data.Length - intSize - guidSize;
        byte[] payload = data.Slice(offset, payloadLength).ToArray();

        EnqueueWalEvent evt = eventType switch
        {
            WalEventType.Enqueue => new EnqueueWalEvent(messageId, payload),
            WalEventType.Requeue => new RequeueWalEvent(messageId),
            _ => throw new UnknownWalEventTypeException()
        };
        
        return evt;
    }
}