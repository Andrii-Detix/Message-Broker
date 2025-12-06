using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.WalReaders.Exceptions;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public class AbstractWalReaderTests : IDisposable
{
    private readonly string _directory;

    public AbstractWalReaderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
    }

    [Fact]
    public void Read_ReturnsAllEvents_WhenFileIsValid()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        
        using (BinaryWriter writer = new(File.OpenWrite(filePath)))
        {
            byte[] data1 = BitConverter.GetBytes(10);
            byte[] header1 = new byte[8];
            crcProvider.WriteHeader(header1, data1);
            writer.Write(header1);
            writer.Write(data1);
            
            byte[] data2 = BitConverter.GetBytes(20);
            byte[] header2 = new byte[8];
            crcProvider.WriteHeader(header2, data2);
            writer.Write(header2);
            writer.Write(data2);
        }
        
        TestWalReader sut = new(crcProvider);
        
        // Act
        TestWalEvent[] actual = sut.Read(filePath).ToArray();
        
        // Assert
        actual.Length.ShouldBe(2);
        actual[0].Value.ShouldBe(10);
        actual[1].Value.ShouldBe(20);
    }
    
    [Fact]
    public void Read_ReturnsEmpty_WhenFileDoesNotExist()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "empty.log");
        ICrcProvider crcProvider = new CrcProvider();
        TestWalReader sut = new(crcProvider);

        // Act
        TestWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.ShouldBeEmpty();
    }

    [Fact]
    public void Read_ReturnsAllCorrectWalEvents_WhenFileIsCorruptedAtTheEnd()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] data1 = BitConverter.GetBytes(10);
            byte[] header1 = new byte[8];
            crcProvider.WriteHeader(header1, data1);
            writer.Write(header1);
            writer.Write(data1);
            
            byte[] data2 = BitConverter.GetBytes(20);
            byte[] header2 = new byte[8];
            crcProvider.WriteHeader(header2, data2);
            writer.Write(header2);
            writer.Write(data2);
            
            writer.Write((byte)1);
        }

        TestWalReader sut = new(crcProvider);

        // Act
        TestWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.Length.ShouldBe(2);
        actual[0].Value.ShouldBe(10);
        actual[1].Value.ShouldBe(20);
    }

    [Fact]
    public void Read_ThrowsException_WhenDataIsCorruptedInTheMiddle()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] data1 = BitConverter.GetBytes(10);
            byte[] header1 = new byte[8];
            crcProvider.WriteHeader(header1, data1);
            writer.Write(header1);
            // Data corruption
            writer.Write(BitConverter.GetBytes(30));
            
            byte[] data2 = BitConverter.GetBytes(20);
            byte[] header2 = new byte[8];
            crcProvider.WriteHeader(header2, data2);
            writer.Write(header2);
            writer.Write(data2);
        }

        TestWalReader sut = new(crcProvider);

        // Act
        Action actual = () => sut.Read(filePath).ToArray();
        
        // Assert
        actual.ShouldThrow<DataCorruptedException>();
    }
    
    [Fact]
    public void Read_ThrowsException_WhenUnexpectedExceptionIsThrownDuringExecution()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        File.Create(filePath).Dispose();
        ICrcProvider crcProvider = new CrcProvider();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] data = BitConverter.GetBytes(10);
            byte[] header = new byte[8];
            crcProvider.WriteHeader(header, data);
            writer.Write(header);
            writer.Write(data);
        }
        
        FaultyTestWalReader sut = new(crcProvider);
        
        // Act
        Action actual = () => sut.Read(filePath).ToArray();
        
        // Assert
        actual.ShouldThrow<CustomException>();
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}