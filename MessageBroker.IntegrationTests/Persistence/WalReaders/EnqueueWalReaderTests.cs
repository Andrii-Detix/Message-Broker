using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WalReaders;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public class EnqueueWalReaderTests : IDisposable
{
    private readonly string _directory;

    public EnqueueWalReaderTests()
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
        byte[] payload = [0xAA, 0xBB, 0xCC];

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Length: 4 (size) + 16 (id) + 3 (payload) = 23
            writer.Write(23); 
            // Id
            writer.Write(messageId.ToByteArray());
            // Payload
            writer.Write(payload);
        }
        
        EnqueueWalReader sut = new();

        // Act
        EnqueueWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.Length.ShouldBe(1);
        EnqueueWalEvent evt = actual.First();
        
        evt.MessageId.ShouldBe(messageId);
        evt.Payload.ShouldBe(payload);
    }
    
    [Fact]
    public void Read_ReturnsEventWithEmptyPayload_WhenPayloadSizeIsZero()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        Guid messageId = Guid.CreateVersion7();
        byte[] payload = [];

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Length: 4 (size) + 16 (id) + 0 (payload) = 20
            writer.Write(20); 
            // Id
            writer.Write(messageId.ToByteArray());
            // Payload
            writer.Write(payload);
        }
        
        EnqueueWalReader sut = new();

        // Act
        EnqueueWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        EnqueueWalEvent evt = actual.First();
        evt.Payload.ShouldBe(payload);
    }

    [Fact]
    public void Read_IgnoresRecord_WhenFileIsCorrupted()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        Guid messageId = Guid.CreateVersion7();

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Length: 4 (size) + 16 (id) + 3 (payload) = 23
            writer.Write(23); 
            // Id
            writer.Write(messageId.ToByteArray());
            // Payload: write only 1 byte
            writer.Write([0x01]);
        }
        
        EnqueueWalReader sut = new();

        // Act
        EnqueueWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.ShouldBeEmpty();
    }

    [Fact]
    public void Read_CanReadMultipleEvents_FromSingleFile()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        
        Guid messageId1 = Guid.CreateVersion7();
        byte[] payload1 = [0xAA, 0xBB, 0xCC];
        
        Guid messageId2 = Guid.CreateVersion7();
        byte[] payload2 = [0xAA, 0xBB];
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            writer.Write(23); 
            writer.Write(messageId1.ToByteArray());
            writer.Write(payload1);
            
            writer.Write(22);
            writer.Write(messageId2.ToByteArray());
            writer.Write(payload2);
        }
        
        EnqueueWalReader sut = new();
        
        // Act
        EnqueueWalEvent[] actual = sut.Read(filePath).ToArray();
        
        // Assert
        actual.Length.ShouldBe(2);
        
        actual[0].MessageId.ShouldBe(messageId1);
        actual[0].Payload.ShouldBe(payload1);
        
        actual[1].MessageId.ShouldBe(messageId2);
        actual[1].Payload.ShouldBe(payload2);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}