using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.FileAppenders;

public class DeadFileAppender(
    IFilePathCreator? filePathCreator, 
    int maxWriteCountPerFile) 
    : AbstractFileAppender<DeadWalEvent>(filePathCreator, maxWriteCountPerFile)
{
    public override void Append(DeadWalEvent evt)
    {
        // Format: message_id (16)
        Span<byte> buffer = stackalloc byte[16];
        
        evt.MessageId.TryWriteBytes(buffer);
        
        SaveData(buffer);
    }
}