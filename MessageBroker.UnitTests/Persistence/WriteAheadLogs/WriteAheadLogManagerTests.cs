using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using MessageBroker.Persistence.WriteAheadLogs;
using MessageBroker.Persistence.WriteAheadLogs.Exceptions;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.WriteAheadLogs;

public class WriteAheadLogManagerTests
{
    private readonly Mock<IFileAppender<EnqueueWalEvent>> _enqueueAppenderMock;
    private readonly Mock<IFileAppender<AckWalEvent>> _ackAppenderMock;
    private readonly Mock<IFileAppender<DeadWalEvent>> _deadAppenderMock;

    public WriteAheadLogManagerTests()
    {
        _enqueueAppenderMock = new();
        _ackAppenderMock = new();
        _deadAppenderMock = new();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenEnqueueAppenderIsNull()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent>? appender = null;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        
        // Act
        Action actual = () => new WriteAheadLogManager(appender, ackAppender, deadAppender);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenAckAppenderIsNull()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent>? ackAppender = null;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        
        // Act
        Action actual = () => new WriteAheadLogManager(appender, ackAppender, deadAppender);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenDeadAppenderIsNull()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent>? deadAppender = null;
        
        // Act
        Action actual = () => new WriteAheadLogManager(appender, ackAppender, deadAppender);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_CreatesWriteAheadLogManager_WhenInputDataIsValid()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        
        // Act
        WriteAheadLogManager actual = new(appender, ackAppender, deadAppender);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Append_RoutesToEnqueueAppender_WhenInputsEnqueueWalEvent()
    {
        // Arrange
        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        
        WriteAheadLogManager sut = CreateSut();
        
        // Act
        sut.Append(evt);
        
        // Assert
        _enqueueAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToEnqueueAppender_WhenInputsRequeueWalEvent()
    {
        // Arrange
        WriteAheadLogManager sut = CreateSut();

        RequeueWalEvent evt = new(Guid.CreateVersion7());
        
        // Act
        sut.Append(evt);
        
        // Assert
        _enqueueAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToAckAppender_WhenInputsAckWalEvent()
    {
        // Arrange
        WriteAheadLogManager sut = CreateSut();
        
        AckWalEvent evt = new(Guid.CreateVersion7());
        
        // Act
        sut.Append(evt);
        
        // Assert
        _ackAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToDeadAppender_WhenInputsDeadWalEvent()
    {
        // Arrange
        WriteAheadLogManager sut = CreateSut();

        DeadWalEvent evt = new(Guid.CreateVersion7());
        
        // Act
        sut.Append(evt);
        
        // Assert
        _deadAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }

    [Fact]
    public void Append_ThrowsException_WhenTypeOfWalEventIsUnknown()
    {
        // Arrange
        WriteAheadLogManager sut = CreateSut();

        TestWalEvent evt = new();
        
        // Act
        Action actual = () => sut.Append(evt);
        
        // Assert
        actual.ShouldThrow<WalEventUnknownTypeException>();
    }

    [Fact]
    public void Append_ThrowsException_WhenExceptionWasThrownDuringWalEventHandling()
    {
        // Arrange
        Exception exception = new("Custom exception");
        
        _enqueueAppenderMock
            .Setup(ea => ea.Append(It.IsAny<EnqueueWalEvent>()))
            .Throws(exception);
        
        WriteAheadLogManager sut = CreateSut();

        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        
        // Act
        Action actual = () => sut.Append(evt);
        
        // Assert
        actual.ShouldThrow<Exception>(exception.Message);
    }

    private WriteAheadLogManager CreateSut()
    {
        return new(
            _enqueueAppenderMock.Object,
            _ackAppenderMock.Object,
            _deadAppenderMock.Object);
    }
}