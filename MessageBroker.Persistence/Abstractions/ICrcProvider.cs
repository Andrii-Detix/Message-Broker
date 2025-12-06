namespace MessageBroker.Persistence.Abstractions;

public interface ICrcProvider
{
    int HeaderSize { get; }
    
    void WriteHeader(Span<byte> destination, ReadOnlySpan<byte> data);

    bool Verify(ReadOnlySpan<byte> data, ReadOnlySpan<byte> crc);

    int GetDataSize(ReadOnlySpan<byte> header);
}