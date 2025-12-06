using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public class DeadWalReaderTests : IDisposable
{
        private readonly string _directory;

    public DeadWalReaderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
    }
    
    [Fact]
    public void Read_ReturnsEventWithPayload_WhenFileFormatIsCorrect()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] idBuffer = messageId.ToByteArray();
            byte[] header = new byte[8];
            crcProvider.WriteHeader(header, idBuffer);
            writer.Write(header);
            writer.Write(idBuffer);
        }
        
        DeadWalReader sut = new(crcProvider);

        // Act
        DeadWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.Length.ShouldBe(1);
        DeadWalEvent evt = actual.First();
        
        evt.MessageId.ShouldBe(messageId);
    }
    
    [Fact]
    public void Read_IgnoresRecord_WhenFileIsCorrupted()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] idBuffer = messageId.ToByteArray();
            byte[] header = new byte[8];
            crcProvider.WriteHeader(header, idBuffer);
            writer.Write(header);
            writer.Write(idBuffer.Take(14).ToArray());
        }
        
        DeadWalReader sut = new(crcProvider);

        // Act
        DeadWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.ShouldBeEmpty();
    }

    [Fact]
    public void Read_CanReadMultipleEvents_FromSingleFile()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId1 = Guid.CreateVersion7();
        Guid messageId2 = Guid.CreateVersion7();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] idBuffer1 = messageId1.ToByteArray();
            byte[] header1 = new byte[8];
            crcProvider.WriteHeader(header1, idBuffer1);
            writer.Write(header1);
            writer.Write(idBuffer1);
            
            byte[] idBuffer2 = messageId2.ToByteArray();
            byte[] header2 = new byte[8];
            crcProvider.WriteHeader(header2, idBuffer2);
            writer.Write(header2);
            writer.Write(idBuffer2);
        }
        
        DeadWalReader sut = new(crcProvider);

        // Act
        DeadWalEvent[] actual = sut.Read(filePath).ToArray();
        
        // Assert
        actual.Length.ShouldBe(2);
        
        actual[0].MessageId.ShouldBe(messageId1);
        actual[1].MessageId.ShouldBe(messageId2);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}