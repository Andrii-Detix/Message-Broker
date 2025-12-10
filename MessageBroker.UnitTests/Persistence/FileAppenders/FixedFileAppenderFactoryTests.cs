using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.FileAppenders;

public class FixedFileAppenderFactoryTests
{
    private readonly Mock<ICrcProvider> _crcMock;
    private readonly Mock<IFilePathCreator> _enqueuePathCreatorMock;
    private readonly Mock<IFilePathCreator> _ackPathCreatorMock;
    private readonly Mock<IFilePathCreator> _deadPathCreatorMock;

    public FixedFileAppenderFactoryTests()
    {
        _crcMock = new();
        _enqueuePathCreatorMock = new();
        _ackPathCreatorMock = new();
        _deadPathCreatorMock = new();
    }

    [Fact]
    public void Create_ReturnsEnqueueAppenderWithCorrectPath_WhenEnqueueWalEventIsInput()
    {
        // Arrange
        string expectedPath = "enqueue-merged-123.log";
        _enqueuePathCreatorMock.Setup(p => p.CreatePath())
            .Returns(expectedPath);

        FixedFileAppenderFactory sut = CreateSut();
        
        // Act
        using IFileAppender<EnqueueWalEvent> actual = sut.Create<EnqueueWalEvent>();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.CurrentFile.ShouldBe(expectedPath);
    }
    
    [Fact]
    public void Create_ReturnsAckAppenderWithCorrectPath_WhenAckWalEventIsInput()
    {
        // Arrange
        string expectedPath = "ack-merged-123.log";
        _ackPathCreatorMock.Setup(p => p.CreatePath())
            .Returns(expectedPath);

        FixedFileAppenderFactory sut = CreateSut();
        
        // Act
        using IFileAppender<AckWalEvent> actual = sut.Create<AckWalEvent>();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.CurrentFile.ShouldBe(expectedPath);
    }
    
    [Fact]
    public void Create_ReturnsDeadAppenderWithCorrectPath_WhenDeadWalEventIsInput()
    {
        // Arrange
        string expectedPath = "dead-merged-123.log";
        _deadPathCreatorMock.Setup(p => p.CreatePath())
            .Returns(expectedPath);

        FixedFileAppenderFactory sut = CreateSut();
        
        // Act
        using IFileAppender<DeadWalEvent> actual = sut.Create<DeadWalEvent>();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.CurrentFile.ShouldBe(expectedPath);
    }

    [Fact]
    public void Create_ThrowsException_WhenUnknownWalEventIsInput()
    {
        // Arrange
        FixedFileAppenderFactory sut = CreateSut();
        
        // Act
        Action actual = () => sut.Create<TestWalEvent>();
        
        // Assert
        actual.ShouldThrow<UnknownWalEventTypeException>();
    }
    
    private FixedFileAppenderFactory CreateSut()
    {
        return new(
            _crcMock.Object,
            _enqueuePathCreatorMock.Object,
            _ackPathCreatorMock.Object,
            _deadPathCreatorMock.Object);
    }
    
    private record TestWalEvent : WalEvent
    {
        public override WalEventType Type => WalEventType.Enqueue;
    }
}