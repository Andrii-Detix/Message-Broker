using System.Buffers.Binary;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using MessageBroker.Persistence.FilePathCreators;
using Microsoft.Extensions.Time.Testing;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.FileAppenders;

public class EnqueueFileAppenderTests : IDisposable
{
    private readonly string _directory;

    public EnqueueFileAppenderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    }

    [Fact]
    public void Append_WritesCorrectBinaryStructure_WhenPayloadIsPresent()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "enqueue", "ext", timeProvider);
        EnqueueFileAppender sut = new(crcProvider, pathCreator, 2);
        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);

        int expectedLength = 8 + 22; // header (8) | record_length (4) | message_id (16) | payload (2)
        
        // Act
        sut.Append(evt);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();

        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedLength);
        
        int actualSize = BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(8, 4));
        actualSize.ShouldBe(expectedLength - 8);
        
        Guid actualMessageId = new Guid(content.AsSpan(12, 16));
        actualMessageId.ShouldBe(evt.MessageId);

        byte[] actualPayload = content.AsSpan(28).ToArray();
        actualPayload.ShouldBe(evt.Payload);
    }
    
    [Fact]
    public void Append_WritesWithoutPayload_WhenPayloadIsEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "enqueue", "ext", timeProvider);
        EnqueueFileAppender sut = new(crcProvider, pathCreator, 2);
        EnqueueWalEvent evt = new(Guid.CreateVersion7(), []);
        
        int expectedLength = 8 + 20; // header (8) | record_length (4) | message_id (16) | payload (0)
        
        // Act
        sut.Append(evt);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();

        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedLength);
        
        int actualSize = BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(8, 4));
        actualSize.ShouldBe(expectedLength - 8);
        
        Guid actualMessageId = new Guid(content.AsSpan(12, 16));
        actualMessageId.ShouldBe(evt.MessageId);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}