using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.FileAppenders;

public class AckFileAppender(
    IFilePathCreator? filePathCreator, 
    int maxWriteCountPerFile) 
    : AbstractFileAppender<AckWalEvent>(filePathCreator, maxWriteCountPerFile)
{
    public override void Append(AckWalEvent evt)
    {
        // Format: message_id (16)
        Span<byte> buffer = stackalloc byte[16];
        
        evt.MessageId.TryWriteBytes(buffer);
        
        SaveData(buffer);
    }
}