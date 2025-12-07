using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
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
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;

        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender);
        
        // Act
        sut.Append(evt);
        
        // Assert
        _enqueueAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToEnqueueAppender_WhenInputsRequeueWalEvent()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;

        RequeueWalEvent evt = new(Guid.CreateVersion7());
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender);
        
        // Act
        sut.Append(evt);
        
        // Assert
        _enqueueAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToAckAppender_WhenInputsAckWalEvent()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;

        AckWalEvent evt = new(Guid.CreateVersion7());
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender);
        
        // Act
        sut.Append(evt);
        
        // Assert
        _ackAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToDeadAppender_WhenInputsDeadWalEvent()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;

        DeadWalEvent evt = new(Guid.CreateVersion7());
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender);
        
        // Act
        sut.Append(evt);
        
        // Assert
        _deadAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }

    [Fact]
    public void Append_ThrowsException_WhenTypeOfWalEventIsUnknown()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;

        TestWalEvent evt = new();
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender);
        
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
        
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;

        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender);
        
        // Act
        Action actual = () => sut.Append(evt);
        
        // Assert
        actual.ShouldThrow<Exception>(exception.Message);
    }
}