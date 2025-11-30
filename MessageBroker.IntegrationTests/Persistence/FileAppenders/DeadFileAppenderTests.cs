using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using MessageBroker.Persistence.FilePathCreators;
using Microsoft.Extensions.Time.Testing;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.FileAppenders;

public class DeadFileAppenderTests : IDisposable
{
    private readonly string _directory;

    public DeadFileAppenderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    }

    [Fact]
    public void Append_WritesMessageIdToBinaryStructure()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "dead", "ext", timeProvider);
        DeadFileAppender sut = new(pathCreator, 2);
        DeadWalEvent evt = new(Guid.CreateVersion7());

        int expectedLength = 16; // message_id (16)
        
        // Act
        sut.Append(evt);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();

        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedLength);
        
        Guid actualMessageId = new Guid(content);
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