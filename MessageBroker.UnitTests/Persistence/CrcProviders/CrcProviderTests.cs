using System.Buffers.Binary;
using System.IO.Hashing;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.CrcProviders.Exceptions;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.CrcProviders;

public class CrcProviderTests
{
    [Fact]
    public void WriteHeader_ThrowsException_WhenDestinationBufferIsTooSmall()
    {
        // Arrange
        byte[] headerBuffer = new byte[7];
        byte[] data = [0x01, 0x02, 0x03];
        
        CrcProvider sut = new();
        
        // Act
        Action actual = () => sut.WriteHeader(headerBuffer, data);
        
        // Assert
        actual.ShouldThrow<DestinationBufferTooSmallException>();
    }

    [Fact]
    public void WriteHeader_WritesCorrectTotalSizeAndCrc()
    {
        // Arrange
        byte[] headerBuffer = new byte[8];
        byte[] data = [0x01, 0x02, 0x03];
        int expectedTotalSize = 8 + 3;
        
        CrcProvider sut = new();
        
        // Act
        sut.WriteHeader(headerBuffer, data);
        
        // Assert
        int actualSize = BinaryPrimitives.ReadInt32LittleEndian(headerBuffer.AsSpan(0, 4));
        actualSize.ShouldBe(expectedTotalSize);
        
        uint expectedCrc = Crc32.HashToUInt32(data);
        uint actualCrc = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(4, 4));
        
        actualCrc.ShouldBe(expectedCrc);
    }

    [Fact]
    public void Verify_ReturnsFalse_WhenHeaderIsTooSmall()
    {
        // Arrange
        ReadOnlySpan<byte> header = [0x01, 0x02];
        CrcProvider sut = new();
        
        // Act
        bool result = sut.Verify([], header);

        // Assert
        result.ShouldBeFalse();
    }

    [Fact]
    public void Verify_ReturnsTrue_WhenCrcMatches()
    {
        // Arrange
        byte[] header = new byte[8];
        byte[] data = [1, 2, 3];
        
        CrcProvider sut = new();
        sut.WriteHeader(header, data);
        
        // Act
        bool result = sut.Verify(data, header);
        
        // Assert
        result.ShouldBeTrue();
    }
    
    [Fact]
    public void Verify_ReturnsFalse_WhenCrcDoesNotMatch()
    {
        // Arrange
        byte[] header = new byte[8];
        byte[] data = [1, 2, 3];
        
        CrcProvider sut = new();
        sut.WriteHeader(header, []);
        
        // Act
        bool result = sut.Verify(data, header);
        
        // Assert
        result.ShouldBeFalse();
    }
    
    [Fact]
    public void Verify_ReturnsFalse_WhenDataIsModified()
    {
        // Arrange
        byte[] data = [1, 2, 3];
        byte[] header = new byte[8];
        
        CrcProvider sut = new();
        sut.WriteHeader(header, data);

        data[0] = 100;

        // Act
        bool result = sut.Verify(data, header);

        // Assert
        result.ShouldBeFalse();
    }
    
    [Fact]
    public void GetDataSize_ThrowsException_WhenHeaderIsTooSmall()
    {
        // Arrange
        byte[] header = [0x01];
        CrcProvider sut = new();

        // Act
        Action act = () => sut.GetDataSize(header);

        // Assert
        act.ShouldThrow<CrcHeaderInvalidSizeException>();
    }
    
    [Fact]
    public void GetDataSize_ThrowsException_WhenTotalSizeIsLessThanHeaderSize()
    {
        // Arrange
        byte[] header = new byte[8];
        CrcProvider sut = new();
        
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, 4), 5);

        // Act
        Action act = () => sut.GetDataSize(header);

        // Assert
        act.ShouldThrow<CorruptedDataSizeException>();
    }
    
    [Fact]
    public void GetDataSize_ReturnsCorrectPayloadSize()
    {
        // Arrange
        int dataSize = 100;
        int totalSize = 8 + dataSize;
        CrcProvider sut = new();
        
        byte[] header = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, 4), totalSize);

        // Act
        int actualSize = sut.GetDataSize(header);

        // Assert
        actualSize.ShouldBe(dataSize);
    }
}