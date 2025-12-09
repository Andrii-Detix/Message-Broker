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
    public void Read_ReturnsEnqueueEventWithPayload_WhenFileFormatIsCorrect()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();
        byte[] payload = [0xAA, 0xBB, 0xCC];

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Event_type
            byte[] eventTypeBuffer = BitConverter.GetBytes((int)WalEventType.Enqueue);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = eventTypeBuffer.Concat(idBuffer).Concat(payload).ToArray();
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
        
        evt.ShouldBeOfType<EnqueueWalEvent>();
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
            // Event_type
            byte[] eventTypeBuffer = BitConverter.GetBytes((int)WalEventType.Enqueue);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = eventTypeBuffer.Concat(idBuffer).Concat(payload).ToArray();
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
            // Event_type
            byte[] eventTypeBuffer = BitConverter.GetBytes((int)WalEventType.Enqueue);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = eventTypeBuffer.Concat(idBuffer).Concat(payload).ToArray();
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
    public void Read_CanHandleRequeueEvent_WhenWalEventTypeIsRequeue()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        ICrcProvider crcProvider = new CrcProvider();
        Guid messageId = Guid.CreateVersion7();

        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            // Event_type
            byte[] eventTypeBuffer = BitConverter.GetBytes((int)WalEventType.Requeue);
            // Id
            byte[] idBuffer = messageId.ToByteArray();
            // Concat
            byte[] data = eventTypeBuffer.Concat(idBuffer).ToArray();
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
        
        evt.ShouldBeOfType<RequeueWalEvent>();
        evt.MessageId.ShouldBe(messageId);
        evt.Payload.ShouldBe([]);
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
        
        Guid messageId3 = Guid.CreateVersion7();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            byte[] lengthBuffer1 = BitConverter.GetBytes((int)WalEventType.Enqueue);
            byte[] idBuffer1 = messageId1.ToByteArray();
            byte[] data1 = lengthBuffer1.Concat(idBuffer1).Concat(payload1).ToArray();
            byte[] header1 = new byte[8];
            crcProvider.WriteHeader(header1, data1);
            writer.Write(header1);
            writer.Write(data1);
            
            byte[] lengthBuffer2 = BitConverter.GetBytes((int)WalEventType.Enqueue);
            byte[] idBuffer2 = messageId2.ToByteArray();
            byte[] data2 = lengthBuffer2.Concat(idBuffer2).Concat(payload2).ToArray();
            byte[] header2 = new byte[8];
            crcProvider.WriteHeader(header2, data2);
            writer.Write(header2);
            writer.Write(data2);
            
            byte[] lengthBuffer3 = BitConverter.GetBytes((int)WalEventType.Requeue);
            byte[] idBuffer3 = messageId3.ToByteArray();
            byte[] data3 = lengthBuffer3.Concat(idBuffer3).ToArray();
            byte[] header3 = new byte[8];
            crcProvider.WriteHeader(header3, data3);
            writer.Write(header3);
            writer.Write(data3);
        }
        
        EnqueueWalReader sut = new(crcProvider);
        
        // Act
        EnqueueWalEvent[] actual = sut.Read(filePath).ToArray();
        
        // Assert
        actual.Length.ShouldBe(3);
        
        actual[0].ShouldBeOfType<EnqueueWalEvent>();
        actual[0].MessageId.ShouldBe(messageId1);
        actual[0].Payload.ShouldBe(payload1);
        
        actual[1].ShouldBeOfType<EnqueueWalEvent>();
        actual[1].MessageId.ShouldBe(messageId2);
        actual[1].Payload.ShouldBe(payload2);
        
        actual[2].ShouldBeOfType<RequeueWalEvent>();
        actual[2].MessageId.ShouldBe(messageId3);
        actual[2].Payload.ShouldBe([]);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}