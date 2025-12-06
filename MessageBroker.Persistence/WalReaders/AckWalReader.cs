using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class AckWalReader(ICrcProvider crcProvider)
    : AbstractWalReader<AckWalEvent>(crcProvider)
{
    protected override AckWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        Guid messageId = new Guid(data);
        
        AckWalEvent evt = new(messageId);
        
        return evt;
    }
}