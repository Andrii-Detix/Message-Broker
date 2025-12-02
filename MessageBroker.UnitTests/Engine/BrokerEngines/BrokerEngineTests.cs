using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Exceptions;
using MessageBroker.Engine.BrokerEngines;
using MessageBroker.Engine.BrokerEngines.Exceptions;
using MessageBroker.Persistence.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.BrokerEngines;

public class BrokerEngineTests
{
    private readonly Mock<IMessageQueue> _queueMock;
    private readonly Mock<IWriteAheadLog> _walMock;
    private readonly Mock<ILogger<BrokerEngine>> _loggerMock;

    public BrokerEngineTests()
    {
        _queueMock = new Mock<IMessageQueue>();
        _walMock = new Mock<IWriteAheadLog>();
        _loggerMock = new Mock<ILogger<BrokerEngine>>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenMessageQueueIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IWriteAheadLog wal = _walMock.Object;
        ILogger<BrokerEngine> logger = _loggerMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            null, 
            wal, 
            timeProvider, 
            maxPayloadLength, 
            maxDeliveryAttempts, 
            logger);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenWriteAheadLogIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        ILogger<BrokerEngine> logger = _loggerMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue, 
            null,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts,
            logger);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        ILogger<BrokerEngine> logger = _loggerMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            null,
            maxPayloadLength,
            maxDeliveryAttempts,
            logger);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenMaxPayloadLengthIsNegative()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        ILogger<BrokerEngine> logger = _loggerMock.Object;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            timeProvider,
            -1,
            maxDeliveryAttempts,
            logger);
        
        // Assert
        actual.ShouldThrow<MaxPayloadLengthNegativeException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_ThrowsException_WhenMaxDeliveryAttemptsIsLessThanOne(int maxDeliveryAttempts)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        ILogger<BrokerEngine> logger = _loggerMock.Object;
        int maxPayloadLength = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts,
            logger);

        // Assert
        actual.ShouldThrow<MaxDeliveryAttemptsInvalidException>();
    }
    
    [Fact]
    public void Constructor_CreatesBrokerEngine_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        ILogger<BrokerEngine> logger = _loggerMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        BrokerEngine actual = new BrokerEngine(
            queue,
            wal,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts,
            logger);
        
        // Assert
        actual.ShouldNotBeNull();
    }
    
    [Fact]
    public void Constructor_CreatesBrokerEngine_WithoutProvidedLogger()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        BrokerEngine actual = new BrokerEngine(
            queue,
            wal,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts);
        
        // Assert
        actual.ShouldNotBeNull();
    }
}