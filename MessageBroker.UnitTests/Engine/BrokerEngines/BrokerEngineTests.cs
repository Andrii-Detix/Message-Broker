using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Exceptions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.BrokerEngines;
using MessageBroker.Engine.BrokerEngines.Exceptions;
using MessageBroker.Engine.Common.Exceptions;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.BrokerEngines;

public class BrokerEngineTests
{
    private readonly Mock<IMessageQueue> _queueMock;
    private readonly Mock<IWriteAheadLog> _walMock;

    public BrokerEngineTests()
    {
        _queueMock = new Mock<IMessageQueue>();
        _walMock = new Mock<IWriteAheadLog>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenMessageQueueIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IWriteAheadLog wal = _walMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            null, 
            wal, 
            timeProvider, 
            maxPayloadLength, 
            maxDeliveryAttempts);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenWriteAheadLogIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue, 
            null,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            null,
            maxPayloadLength,
            maxDeliveryAttempts);
        
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
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            timeProvider,
            -1,
            maxDeliveryAttempts);
        
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
        int maxPayloadLength = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts);

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
    
    [Fact]
    public void Publish_ThrowsException_WhenPayloadIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Action actual = () => sut.Publish(null!);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Publish_ThrowsException_WhenPayloadSizeIsTooLarge()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        int maxPayloadLength = 1;
        int maxDeliveryAttempts = 5;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Action actual = () => sut.Publish([0x01, 0x02]);
        
        // Assert
        actual.ShouldThrow<PayloadTooLargeException>();
    }

    [Fact]
    public void Publish_ThrowsException_WhenFailureHasOccurredDuringStoringEnqueueEvent()
    {
        // Arrange
        _walMock.Setup(w => w.Append(It.IsAny<WalEvent>()))
            .Throws(new BrokerStorageException());
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(true);
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Action actual = () => sut.Publish([0x01, 0x02]);
        
        // Assert
        actual.ShouldThrow<BrokerStorageException>();
    }

    [Fact]
    public void Publish_AppendsCompensatingDeadEvent_WhenFailureHasOccurredDuringAppendingEnqueueEvent()
    {
        // Arrange
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(false);
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Action actual = () => sut.Publish([0x01, 0x02]);
        
        // Assert
        actual.ShouldThrow<MessageQueueEnqueueException>();
        _walMock.Verify(w => w.Append(It.IsAny<DeadWalEvent>()), Times.Once);
    }

    [Fact]
    public void Publish_AppendsEnqueueEventAndEnqueuesMessage_WhenNoErrorHasOccurredDuringPublishing()
    {
        // Arrange
        EnqueueWalEvent? capturedEvent = null;
        Message? capturedMessage = null;

        _walMock.Setup(w => w.Append(It.IsAny<EnqueueWalEvent>()))
            .Callback<WalEvent>(evt => capturedEvent = evt as EnqueueWalEvent);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Callback<Message>(message => capturedMessage = message)
            .Returns(true);
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        sut.Publish([0x01, 0x02]);
        
        // Assert
        capturedEvent.ShouldNotBeNull();
        capturedMessage.ShouldNotBeNull();
        capturedEvent.Payload.ShouldBe([0x01, 0x02]);
        capturedMessage.Payload.ShouldBe([0x01, 0x02]);
        capturedEvent.MessageId.ShouldBe(capturedMessage.Id);
        
        _walMock.Verify(w => w.Append(It.IsAny<EnqueueWalEvent>()), Times.Once);
        _queueMock.Verify(q => q.TryEnqueue(It.IsAny<Message>()), Times.Once);
    }

    [Fact]
    public void Publish_StoresEventsAndEnqueuesMessagesInTheSameOrder_WhenPublishMessagesConcurrently()
    {
        // Arrange
        ConcurrentQueue<Guid> walMessageIds = [];
        ConcurrentQueue<Guid> queueMessageIds = [];

        _walMock.Setup(w => w.Append(It.IsAny<EnqueueWalEvent>()))
            .Callback<WalEvent>(evt => walMessageIds.Enqueue((evt as EnqueueWalEvent)!.MessageId));
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Callback<Message>(message => queueMessageIds.Enqueue(message.Id))
            .Returns(true);
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);

        int threadCount = 1000;
        int publishesPerThread = 100;

        // Act
        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < publishesPerThread; i++)
            {
                sut.Publish([0x01, 0x02]);
            }
        });
        
        // Assert
        walMessageIds.Count.ShouldBe(threadCount * publishesPerThread);
        queueMessageIds.Count.ShouldBe(threadCount * publishesPerThread);
        walMessageIds.ShouldBe(queueMessageIds, ignoreOrder: false);
    }

    [Fact]
    public void Consume_ReturnsMessage_WhenQueueIsNotEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message? expectedMessage = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        
        _queueMock.Setup(q => q.TryConsume(out expectedMessage))
            .Returns(true);
        
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Message? actual = sut.Consume();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBe(expectedMessage);
    }
    
    [Fact]
    public void Consume_ReturnsNull_WhenQueueIsEmpty()
    {
        // Arrange
        Message? expectedMessage = null;
        
        _queueMock.Setup(q => q.TryConsume(out expectedMessage))
            .Returns(false);
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Message? actual = sut.Consume();
        
        // Assert
        actual.ShouldBeNull();
    }

    [Fact]
    public void Ack_ThrowsException_WhenFailureHasOccurredDuringAppendingAckEvent()
    {
        // Arrange
        _walMock.Setup(w => w.Append(It.IsAny<AckWalEvent>()))
            .Throws(new BrokerStorageException());
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        Guid messageId = Guid.CreateVersion7();
        
        // Act
        Action actual = () => sut.Ack(messageId);
        
        // Assert
        actual.ShouldThrow<BrokerStorageException>();
        
        _queueMock.Verify(q => q.Ack(messageId), Times.Never);
    }

    [Fact]
    public void Ack_ThrowsException_WhenThereIsNoInFlightMessageWithInputId()
    {
        // Arrange
        _queueMock.Setup(q => q.Ack(It.IsAny<Guid>()))
            .Returns((Message?)null);
        
        FakeTimeProvider timeProvider = new();
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        Action actual = () => sut.Ack(Guid.CreateVersion7());
        
        // Assert
        actual.ShouldThrow<SentMessageNotFoundException>();
    }
    
    [Fact]
    public void Ack_AppendsAckEventAndAcknowledgesMessage_WhenInFlightMessageWithInputIdExists()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid messageId = Guid.CreateVersion7();
        Message message = Message.Create(messageId, [], 1, timeProvider);
        
        _queueMock.Setup(q => q.Ack(messageId))
            .Returns(message);
        
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        BrokerEngine sut = new(queue, wal, timeProvider, maxPayloadLength, maxDeliveryAttempts);
        
        // Act
        sut.Ack(messageId);
        
        // Assert
        _walMock.Verify(w => w.Append(It.Is<AckWalEvent>(e => e.MessageId == messageId)), Times.Once);
        _queueMock.Verify(q => q.Ack(messageId), Times.Once);
    }
}