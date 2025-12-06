using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders;
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
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();
        byte[] payload = [0xAA, 0xBB, 0xCC];

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Length: 4 (size) + 16 (id) + 3 (payload) = 23
            byte[] lengthBuffer = BitConverter.GetBytes(23);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = lengthBuffer.Concat(idBuffer).Concat(payload).ToArray();
            // Header
            byte[] header = new byte[8];
            crcProvider.WriteHeader(header, data);

            writer.Write(header);
            writer.Write(data);
        }
        
        EnqueueWalReader sut = new(crcProvider);

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
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();
        byte[] payload = [];

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Length: 4 (size) + 16 (id) + 0 (payload) = 20
            byte[] lengthBuffer = BitConverter.GetBytes(20);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = lengthBuffer.Concat(idBuffer).Concat(payload).ToArray();
            // Header
            byte[] header = new byte[8];
            crcProvider.WriteHeader(header, data);

            writer.Write(header);
            writer.Write(data);
        }
        
        EnqueueWalReader sut = new(crcProvider);

        // Act
        EnqueueWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        EnqueueWalEvent evt = actual.First();
        evt.Payload.ShouldBe(payload);
    }

    [Fact]
    public void Read_IgnoresLastRecord_WhenFileIsCorruptedAtTheEnd()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();
        byte[] payload = [0xAA, 0xBB, 0xCC];

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Length: 4 (size) + 16 (id) + 3 (payload) = 23
            byte[] lengthBuffer = BitConverter.GetBytes(23);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = lengthBuffer.Concat(idBuffer).Concat(payload).ToArray();
            // Header
            byte[] header = new byte[8];
            crcProvider.WriteHeader(header, data);

            writer.Write(header);
            // Write only 1 byte
            writer.Write([0x01]);
        }
        
        EnqueueWalReader sut = new(crcProvider);

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
        ICrcProvider crcProvider = new CrcProvider();
        
        Guid messageId1 = Guid.CreateVersion7();
        byte[] payload1 = [0xAA, 0xBB, 0xCC];
        
        Guid messageId2 = Guid.CreateVersion7();
        byte[] payload2 = [0xAA, 0xBB];
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] lengthBuffer1 = BitConverter.GetBytes(23);
            byte[] idBuffer1 = messageId1.ToByteArray();
            byte[] data1 = lengthBuffer1.Concat(idBuffer1).Concat(payload1).ToArray();
            byte[] header1 = new byte[8];
            crcProvider.WriteHeader(header1, data1);
            writer.Write(header1);
            writer.Write(data1);
            
            byte[] lengthBuffer2 = BitConverter.GetBytes(22);
            byte[] idBuffer2 = messageId2.ToByteArray();
            byte[] data2 = lengthBuffer2.Concat(idBuffer2).Concat(payload2).ToArray();
            byte[] header2 = new byte[8];
            crcProvider.WriteHeader(header2, data2);
            writer.Write(header2);
            writer.Write(data2);
        }
        
        EnqueueWalReader sut = new(crcProvider);
        
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