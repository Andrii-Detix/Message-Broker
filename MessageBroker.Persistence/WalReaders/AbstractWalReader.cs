using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;

namespace MessageBroker.Persistence.WalReaders;

public abstract class AbstractWalReader<TEvent> 
    : IWalReader<TEvent> 
    where TEvent : WalEvent
{
    public IEnumerable<TEvent> Read(string filePath)
    {
        if (!File.Exists(filePath))
        {
            yield break;
        }
        
        using FileStream stream = new(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using BinaryReader reader = new(stream);

        while (TryReadNextSafely(reader, out TEvent? evt))
        {
            yield return evt!;
        }
    }
    
    protected abstract bool TryReadNext(BinaryReader reader, out TEvent? evt);

    protected bool CanRead(Stream stream, int length)
    {
        return stream.Length - stream.Position >= length;
    }
    
    private bool TryReadNextSafely(BinaryReader reader, out TEvent? evt)
    {
        try
        {
            return TryReadNext(reader, out evt);
        }
        catch (EndOfStreamException)
        {
            evt = null;
            return false;
        }
    }
}