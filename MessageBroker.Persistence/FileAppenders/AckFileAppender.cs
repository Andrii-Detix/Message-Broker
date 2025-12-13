using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Constants;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.FileAppenders;

public class AckFileAppender(
    ICrcProvider? crcProvider,
    IFilePathCreator? filePathCreator, 
    int maxWriteCountPerFile) 
    : AbstractFileAppender<AckWalEvent>(crcProvider, filePathCreator, maxWriteCountPerFile)
{
    public override void Append(AckWalEvent evt)
    {
        // Format: message_id (16)
        Span<byte> buffer = stackalloc byte[BinaryLayout.GuidSize];
        
        evt.MessageId.TryWriteBytes(buffer);
        
        SaveData(buffer);
    }
}