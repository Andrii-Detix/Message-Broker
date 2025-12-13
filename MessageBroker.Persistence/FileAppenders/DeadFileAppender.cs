using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Constants;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.FileAppenders;

public class DeadFileAppender(
    ICrcProvider? crcProvider,
    IFilePathCreator? filePathCreator, 
    int maxWriteCountPerFile) 
    : AbstractFileAppender<DeadWalEvent>(crcProvider, filePathCreator, maxWriteCountPerFile)
{
    public override void Append(DeadWalEvent evt)
    {
        // Format: message_id (16)
        Span<byte> buffer = stackalloc byte[BinaryLayout.GuidSize];
        
        evt.MessageId.TryWriteBytes(buffer);
        
        SaveData(buffer);
    }
}