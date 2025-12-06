using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using MessageBroker.Persistence.FilePathCreators;
using Microsoft.Extensions.Time.Testing;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.FileAppenders;

public class AckFileAppenderTests : IDisposable
{
    private readonly string _directory;

    public AckFileAppenderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    }

    [Fact]
    public void Append_WritesMessageIdToBinaryStructure()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "ack", "ext", timeProvider);
        AckFileAppender sut = new(crcProvider, pathCreator, 2);
        AckWalEvent evt = new(Guid.CreateVersion7());

        int expectedLength = 8 + 16; // header (8) + message_id (16)
        
        // Act
        sut.Append(evt);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();

        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedLength);

        byte[] idBuffer = content.AsSpan(8).ToArray();
        Guid actualMessageId = new Guid(idBuffer);
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