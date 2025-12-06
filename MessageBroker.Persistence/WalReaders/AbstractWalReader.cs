using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders.Exceptions;

namespace MessageBroker.Persistence.WalReaders;

public abstract class AbstractWalReader<TEvent> 
    : IWalReader<TEvent> 
    where TEvent : WalEvent
{
    private readonly ICrcProvider _crcProvider;

    protected AbstractWalReader(ICrcProvider crcProvider)
    {
        ArgumentNullException.ThrowIfNull(crcProvider);
        
        _crcProvider = crcProvider;
    }
    
    public IEnumerable<TEvent> Read(string filePath)
    {
        if (!File.Exists(filePath))
        {
            yield break;
        }
        
        using FileStream stream = new(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using BinaryReader reader = new(stream);

        while (TryReadNext(reader, out TEvent? evt))
        {
            yield return evt!;
        }
    }
    
    protected abstract TEvent ParseToEvent(ReadOnlySpan<byte> data);

    protected bool CanRead(Stream stream, int length)
    {
        return stream.Length - stream.Position >= length;
    }

    private bool TryReadNext(BinaryReader reader, out TEvent? evt)
    {
        evt = null;
        
        int headerSize = _crcProvider.HeaderSize;

        if (!CanRead(reader.BaseStream, headerSize))
        {
            return false;
        }
        
        ReadOnlySpan<byte> header = reader.ReadBytes(headerSize);
        int dataSize = _crcProvider.GetDataSize(header);

        if (!CanRead(reader.BaseStream, dataSize))
        {
            return false;
        }

        ReadOnlySpan<byte> data = reader.ReadBytes(dataSize);

        bool isValid = _crcProvider.Verify(data, header);

        if (!isValid)
        {
            throw new DataCorruptedException();
        }
        
        evt = ParseToEvent(data);
        return true;
    }
}