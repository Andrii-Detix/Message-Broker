using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public class DeadWalReader(ICrcProvider crcProvider)
    : AbstractWalReader<DeadWalEvent>(crcProvider)
{
    protected override DeadWalEvent ParseToEvent(ReadOnlySpan<byte> data)
    {
        Guid messageId = new Guid(data);
        
        DeadWalEvent evt = new(messageId);
        
        return evt;
    }
}