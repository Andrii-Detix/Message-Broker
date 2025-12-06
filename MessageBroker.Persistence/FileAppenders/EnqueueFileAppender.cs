using System.Buffers.Binary;
using MessageBroker.Persistence.Abstractions;
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
        
        // Format: record_length (4) | message_id (16) | payload (n-bytes)
        int length = 4 + 16 + payloadLength;

        byte[] bufferArray = new byte[length];
        Span<byte> buffer = bufferArray;
        
        // record_length (4)
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(0, 4), length);
        
        // message_id (16)
        evt.MessageId.TryWriteBytes(buffer.Slice(4, 16));
        
        // payload (n-bytes)
        evt.Payload.CopyTo(buffer.Slice(20));
        
        SaveData(buffer);
    }
}