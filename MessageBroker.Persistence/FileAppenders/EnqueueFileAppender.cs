using System.Buffers.Binary;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Constants;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.FileAppenders;

public class EnqueueFileAppender(
    ICrcProvider? crcProvider,
    IFilePathCreator? filePathCreator, 
    int maxWriteCountPerFile) 
    : AbstractFileAppender<EnqueueWalEvent>(crcProvider, filePathCreator, maxWriteCountPerFile)
{
    public override void Append(EnqueueWalEvent evt)
    {
        int payloadLength = evt.Payload.Length;
        
        // Format: event_type (4) | message_id (16) | payload (n-bytes)
        int length = BinaryLayout.IntSize + BinaryLayout.GuidSize + payloadLength;

        byte[] bufferArray = new byte[length];
        Span<byte> buffer = bufferArray;

        int offset = 0;
        
        // record_length (4)
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(offset, BinaryLayout.IntSize), (int)evt.Type);
        offset += BinaryLayout.IntSize;
        
        // message_id (16)
        evt.MessageId.TryWriteBytes(buffer.Slice(offset, BinaryLayout.GuidSize));
        offset += BinaryLayout.GuidSize;
        
        // payload (n-bytes)
        evt.Payload.CopyTo(buffer.Slice(offset));
        
        SaveData(buffer);
    }
}