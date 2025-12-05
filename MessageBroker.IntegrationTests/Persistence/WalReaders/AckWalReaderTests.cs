using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public class AckWalReaderTests : IDisposable
{
    private readonly string _directory;

    public AckWalReaderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
    }
    
    [Fact]
    public void Read_ReturnsEventWithPayload_WhenFileFormatIsCorrect()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        Guid messageId = Guid.CreateVersion7();

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            writer.Write(messageId.ToByteArray());
        }
        
        AckWalReader sut = new();

        // Act
        AckWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.Length.ShouldBe(1);
        AckWalEvent evt = actual.First();
        
        evt.MessageId.ShouldBe(messageId);
    }
    
    [Fact]
    public void Read_IgnoresRecord_WhenFileIsCorrupted()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        Guid messageId = Guid.CreateVersion7();
        
        byte[] data = messageId
            .ToByteArray()
            .Take(14)
            .ToArray();

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            writer.Write(data);
        }
        
        AckWalReader sut = new();

        // Act
        AckWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.ShouldBeEmpty();
    }

    [Fact]
    public void Read_CanReadMultipleEvents_FromSingleFile()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        Guid messageId1 = Guid.CreateVersion7();
        Guid messageId2 = Guid.CreateVersion7();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            writer.Write(messageId1.ToByteArray());
            writer.Write(messageId2.ToByteArray());
        }
        
        AckWalReader sut = new();

        // Act
        AckWalEvent[] actual = sut.Read(filePath).ToArray();
        
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