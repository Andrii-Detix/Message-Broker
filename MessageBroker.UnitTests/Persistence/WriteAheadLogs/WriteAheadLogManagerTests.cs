using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.WriteAheadLogs;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.WriteAheadLogs;

public class WriteAheadLogManagerTests
{
    private readonly Mock<IFileAppender<EnqueueWalEvent>> _enqueueAppenderMock;
    private readonly Mock<IFileAppender<AckWalEvent>> _ackAppenderMock;
    private readonly Mock<IFileAppender<DeadWalEvent>> _deadAppenderMock;
    private readonly Mock<ILogger<WriteAheadLogManager>> _loggerMock;

    public WriteAheadLogManagerTests()
    {
        _enqueueAppenderMock = new();
        _ackAppenderMock = new();
        _deadAppenderMock = new();
        _loggerMock = new();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenEnqueueAppenderIsNull()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent>? appender = null;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;
        
        // Act
        Action actual = () => new WriteAheadLogManager(appender, ackAppender, deadAppender, logger);
        
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
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;
        
        // Act
        Action actual = () => new WriteAheadLogManager(appender, ackAppender, deadAppender, logger);
        
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
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;
        
        // Act
        Action actual = () => new WriteAheadLogManager(appender, ackAppender, deadAppender, logger);
        
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
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;
        
        // Act
        WriteAheadLogManager actual = new(appender, ackAppender, deadAppender, logger);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Constructor_CreatesWriteAheadLogManager_WhenLoggerIsNull()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager>? logger = null;
        
        // Act
        WriteAheadLogManager actual = new(appender, ackAppender, deadAppender, logger);
        
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
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        bool actual = sut.Append(evt);
        
        // Assert
        actual.ShouldBeTrue();
        _enqueueAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToEnqueueAppender_WhenInputsRequeueWalEvent()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        RequeueWalEvent evt = new(Guid.CreateVersion7());
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        bool actual = sut.Append(evt);
        
        // Assert
        actual.ShouldBeTrue();
        _enqueueAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToAckAppender_WhenInputsAckWalEvent()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        AckWalEvent evt = new(Guid.CreateVersion7());
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        bool actual = sut.Append(evt);
        
        // Assert
        actual.ShouldBeTrue();
        _ackAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }
    
    [Fact]
    public void Append_RoutesToDeadAppender_WhenInputsDeadWalEvent()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        DeadWalEvent evt = new(Guid.CreateVersion7());
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        bool actual = sut.Append(evt);
        
        // Assert
        actual.ShouldBeTrue();
        _deadAppenderMock.Verify(ea => ea.Append(evt), Times.Once);
    }

    [Fact]
    public void Append_ReturnsFalse_WhenTypeOfWalEventIsUnknown()
    {
        // Arrange
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        TestWalEvent evt = new();
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        bool actual = sut.Append(evt);
        
        // Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Append_ReturnsFalse_WhenExceptionWasThrownDuringWalEventHandling()
    {
        // Arrange
        _enqueueAppenderMock
            .Setup(ea => ea.Append(It.IsAny<EnqueueWalEvent>()))
            .Throws(new Exception());
        
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        bool actual = sut.Append(evt);
        
        // Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void Append_CallsLogger_WhenExceptionWasThrownDuringWalEventHandling()
    {
        // Arrange
        Exception ex = new Exception();
        _enqueueAppenderMock
            .Setup(ea => ea.Append(It.IsAny<EnqueueWalEvent>()))
            .Throws(ex);
        
        IFileAppender<EnqueueWalEvent> appender = _enqueueAppenderMock.Object;
        IFileAppender<AckWalEvent> ackAppender = _ackAppenderMock.Object;
        IFileAppender<DeadWalEvent> deadAppender = _deadAppenderMock.Object;
        ILogger<WriteAheadLogManager> logger = _loggerMock.Object;

        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01, 0x02]);
        WriteAheadLogManager sut = new(appender, ackAppender, deadAppender, logger);
        
        // Act
        sut.Append(evt);
        
        // Assert
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => true),
                ex,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()
            ),
            Times.Once
        );
    }
}