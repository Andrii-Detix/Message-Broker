using System.Buffers.Binary;
using System.IO.Hashing;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders.Exceptions;

namespace MessageBroker.Persistence.CrcProviders;

public class CrcProvider : ICrcProvider
{
    public int HeaderSize => 8;
    
    public void WriteHeader(Span<byte> destination, ReadOnlySpan<byte> data)
    {
        if (destination.Length < HeaderSize)
        {
            throw new DestinationBufferTooSmallException(HeaderSize);
        }
        
        int totalSize = HeaderSize + data.Length;
        
        uint crc = Crc32.HashToUInt32(data);
        
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(0, 4), totalSize);
        
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(4, 4), crc);
    }

    public bool Verify(ReadOnlySpan<byte> data, ReadOnlySpan<byte> header)
    {
        if (header.Length < HeaderSize)
        {
            return false;
        }
        
        uint expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header.Slice(4, 4));
        
        uint actualCrc = Crc32.HashToUInt32(data);
        
        return actualCrc == expectedCrc;
    }

    public int GetDataSize(ReadOnlySpan<byte> header)
    {
        if (header.Length < HeaderSize)
        {
            throw new CrcHeaderInvalidSizeException(HeaderSize);
        }
        
        int totalSize = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(0, 4));

        if (totalSize < HeaderSize)
        {
            throw new CorruptedDataSizeException(totalSize, HeaderSize);
        }
        
        return totalSize - HeaderSize;
    }
}