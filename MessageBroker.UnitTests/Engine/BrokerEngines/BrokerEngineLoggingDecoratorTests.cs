using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.Decorators.BrokerEngine;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.BrokerEngines;

public class BrokerEngineLoggingDecoratorTests
{
    private readonly Mock<IBrokerEngine> _innerEngineMock;
    private readonly Mock<ILogger<BrokerEngineLoggingDecorator>> _loggerMock;

    public BrokerEngineLoggingDecoratorTests()
    {
        _innerEngineMock = new ();
        _loggerMock = new();
    }

    [Fact]
    public void Publish_CallsInnerEngineAndLogsSuccess_WhenOperationCompletedSuccessfully()
    {
        // Arrange
        byte[] payload = [0x01, 0x02];
        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        sut.Publish(payload);
        
        // Assert
        _innerEngineMock.Verify(e => e.Publish(payload), Times.Once); 
        VerifyLog(LogLevel.Debug, Times.Exactly(2));
    }

    [Fact]
    public void Publish_LogsErrorAndThrowsException_WhenOperationFailed()
    {
        // Arrange
        byte[] payload = [0x01, 0x02];
        
        _innerEngineMock.Setup(e => e.Publish(payload))
            .Throws(new Exception("Custom exception."));

        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        Action actual = () => sut.Publish(payload);
        
        // Assert
        actual.ShouldThrow<Exception>("Custom exception.");
        
        _innerEngineMock.Verify(e => e.Publish(payload), Times.Once); 
        VerifyLog(LogLevel.Debug, Times.Exactly(1));
        VerifyLog(LogLevel.Error, Times.Exactly(1));
    }

    [Fact]
    public void Consume_ReturnsMessageAndLogsSuccess_WhenOperationCompletedSuccessfully()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        
        _innerEngineMock.Setup(e => e.Consume())
            .Returns(message);

        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        Message? actual = sut.Consume();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBe(message);
        
        _innerEngineMock.Verify(e => e.Consume(), Times.Once);
        VerifyLog(LogLevel.Debug, Times.Exactly(2));
    }
    
    [Fact]
    public void Consume_ReturnsNullAndLogsSuccess_WhenInnerEngineReturnsNull()
    {
        // Arrange
        _innerEngineMock.Setup(e => e.Consume())
            .Returns((Message?)null);

        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        Message? actual = sut.Consume();
        
        // Assert
        actual.ShouldBeNull();
        
        _innerEngineMock.Verify(e => e.Consume(), Times.Once);
        VerifyLog(LogLevel.Debug, Times.Exactly(2));
    }

    [Fact]
    public void Consume_LogsErrorAndThrowsException_WhenInnerEngineThrowsException()
    {
        // Arrange
        _innerEngineMock.Setup(e => e.Consume())
            .Throws(new Exception("Custom exception."));

        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        Action actual = () => sut.Consume();
        
        // Assert
        actual.ShouldThrow<Exception>("Custom exception.");
        
        _innerEngineMock.Verify(e => e.Consume(), Times.Once); 
        VerifyLog(LogLevel.Debug, Times.Exactly(1));
        VerifyLog(LogLevel.Error, Times.Exactly(1));
    }

    [Fact]
    public void Ack_CallsInnerEngineAndLogsSuccess_WhenOperationCompletedSuccessfully()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        sut.Ack(messageId);
        
        // Assert
        _innerEngineMock.Verify(e => e.Ack(messageId), Times.Once);
        VerifyLog(LogLevel.Debug, Times.Exactly(2));
    }

    [Fact]
    public void Ack_LogsErrorAndThrowsException_WhenOperationFailed()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        
        _innerEngineMock.Setup(e => e.Ack(messageId))
            .Throws(new Exception("Custom exception."));

        BrokerEngineLoggingDecorator sut = CreateSut();
        
        // Act
        Action actual = () => sut.Ack(messageId);
        
        // Assert
        actual.ShouldThrow<Exception>("Custom exception.");
        
        _innerEngineMock.Verify(e => e.Ack(messageId), Times.Once); 
        VerifyLog(LogLevel.Debug, Times.Exactly(1));
        VerifyLog(LogLevel.Error, Times.Exactly(1));
    }
    
    private void VerifyLog(LogLevel level, Times times)
    {
        _loggerMock.Verify(
            x => x.Log(
                level,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => true),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()
            ),
            times
        );
    }

    private BrokerEngineLoggingDecorator CreateSut()
    {
        return new(_innerEngineMock.Object, _loggerMock.Object);
    }
}