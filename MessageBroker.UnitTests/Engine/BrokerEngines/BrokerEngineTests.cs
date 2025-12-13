using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Exceptions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.BrokerEngines;
using MessageBroker.Engine.BrokerEngines.Exceptions;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.BrokerEngines;

public class BrokerEngineTests
{
    private readonly Mock<IMessageQueue> _queueMock;
    private readonly Mock<IWriteAheadLog> _walMock;
    private readonly Mock<IExpiredMessagePolicy> _expiredPolicyMock;
    private readonly FakeTimeProvider _timeProvider;

    public BrokerEngineTests()
    {
        _queueMock = new();
        _walMock = new();
        _expiredPolicyMock = new();
        _timeProvider = new();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenMessageQueueIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IWriteAheadLog wal = _walMock.Object;
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            null, 
            wal, 
            expiredPolicy,
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
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue, 
            null,
            expiredPolicy,
            timeProvider,
            maxPayloadLength,
            maxDeliveryAttempts);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenExpiredPolicyIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue, 
            wal,
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
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            expiredPolicy,
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
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            expiredPolicy,
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
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        int maxPayloadLength = 5;
        
        // Act
        Action actual = () => new BrokerEngine(
            queue,
            wal,
            expiredPolicy,
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
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        int maxPayloadLength = 5;
        int maxDeliveryAttempts = 5;
        
        // Act
        BrokerEngine actual = new BrokerEngine(
            queue,
            wal,
            expiredPolicy,
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
        BrokerEngine sut = CreateSut();
        
        // Act
        Action actual = () => sut.Publish(null!);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Publish_ThrowsException_WhenPayloadSizeIsTooLarge()
    {
        // Arrange
        int maxPayloadLength = 1;
        BrokerEngine sut = CreateSut(maxPayloadLength: maxPayloadLength);
        
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
            .Throws(new WalStorageException("Custom exception"));
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(true);

        BrokerEngine sut = CreateSut();
        
        // Act
        Action actual = () => sut.Publish([0x01, 0x02]);
        
        // Assert
        actual.ShouldThrow<WalStorageException>();
    }

    [Fact]
    public void Publish_AppendsCompensatingDeadEvent_WhenFailureHasOccurredDuringAppendingEnqueueEvent()
    {
        // Arrange
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(false);

        BrokerEngine sut = CreateSut();
        
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
        
        BrokerEngine sut = CreateSut();
        
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
        
        BrokerEngine sut = CreateSut();

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
        Message? expectedMessage = Message.Create(Guid.CreateVersion7(), [], 1, _timeProvider);
        
        _queueMock.Setup(q => q.TryConsume(out expectedMessage))
            .Returns(true);
        
        BrokerEngine sut = CreateSut();
        
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
        
        BrokerEngine sut = CreateSut();
        
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
            .Throws(new WalStorageException("Custom exception"));
        
        BrokerEngine sut = CreateSut();
        
        Guid messageId = Guid.CreateVersion7();
        
        // Act
        Action actual = () => sut.Ack(messageId);
        
        // Assert
        actual.ShouldThrow<WalStorageException>();
        
        _queueMock.Verify(q => q.Ack(messageId), Times.Never);
    }

    [Fact]
    public void Ack_ThrowsException_WhenThereIsNoInFlightMessageWithInputId()
    {
        // Arrange
        _queueMock.Setup(q => q.Ack(It.IsAny<Guid>()))
            .Returns((Message?)null);
        
        BrokerEngine sut = CreateSut();
        
        // Act
        Action actual = () => sut.Ack(Guid.CreateVersion7());
        
        // Assert
        actual.ShouldThrow<SentMessageNotFoundException>();
    }
    
    [Fact]
    public void Ack_AppendsAckEventAndAcknowledgesMessage_WhenInFlightMessageWithInputIdExists()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        Message message = Message.Create(messageId, [], 1, _timeProvider);
        
        _queueMock.Setup(q => q.Ack(messageId))
            .Returns(message);
        
        BrokerEngine sut = CreateSut();
        
        // Act
        sut.Ack(messageId);
        
        // Assert
        _walMock.Verify(w => w.Append(It.Is<AckWalEvent>(e => e.MessageId == messageId)), Times.Once);
        _queueMock.Verify(q => q.Ack(messageId), Times.Once);
    }
    
    [Fact]
    public void Requeue_AppendsRequeueEventAndEnqueuesMessage_WhenEverythingSucceeds()
    {
        // Arrange
        Message[] messages = Enumerable
            .Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 2, _timeProvider))
            .ToArray();
        
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns(messages);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(true);
        
        BrokerEngine sut = CreateSut(expiredPolicy: expiredPolicy);
        
        // Act
        sut.Requeue();
        
        // Assert
        _queueMock.Verify(q => q.TakeExpiredMessages(expiredPolicy), Times.Once);
        _walMock.Verify(w => w.Append(It.IsAny<RequeueWalEvent>()), Times.Exactly(100));
        _queueMock.Verify(q => q.TryEnqueue(It.IsAny<Message>()), Times.Exactly(100));
        
        foreach (var message in messages)
        {
            RequeueWalEvent requeueEvent = new(message.Id);
            _walMock.Verify(w => w.Append(requeueEvent), Times.Once);
            _queueMock.Verify(q => q.TryEnqueue(message), Times.Once);
        }
        
        _walMock.Verify(w => w.Append(It.IsAny<DeadWalEvent>()), Times.Never);
    }

    [Fact]
    public void Requeue_AppendsDeadWalEvent_WhenQueueRejectsMessage()
    {
        // Arrange
        Message message = Message.Create(Guid.CreateVersion7(), [], 2, _timeProvider);
        
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns([message]);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(false);
        
        BrokerEngine sut = CreateSut(expiredPolicy: expiredPolicy);
        
        // Act
        sut.Requeue();
        
        // Assert
        DeadWalEvent deadEvent = new(message.Id);
        _walMock.Verify(w => w.Append(deadEvent), Times.Once);
    }
    
    [Fact]
    public void Requeue_AppendsDeadWalEventsOnlyForRejectedMessages_WhenNotAllMessagesAreRejected()
    {
        // Arrange
        Message[] messages = Enumerable
            .Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 2, _timeProvider))
            .ToArray();
        Message[] rejectedMessages = messages.Skip(10).Take(10).ToArray();
        
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns(messages);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns((Message message) => !rejectedMessages.Contains(message));
        
        BrokerEngine sut = CreateSut(expiredPolicy: expiredPolicy);
        
        // Act
        sut.Requeue();
        
        // Assert
        _queueMock.Verify(q => q.TryEnqueue(It.IsAny<Message>()), Times.Exactly(100));
        _walMock.Verify(w => w.Append(It.IsAny<DeadWalEvent>()), Times.Exactly(10));

        foreach (var message in rejectedMessages)
        {
            DeadWalEvent deadEvent = new(message.Id);
            _walMock.Verify(w => w.Append(deadEvent), Times.Once);
        }
    }
    
    [Fact]
    public async Task BrokerEngine_StoresEventsAndEnqueueMessagesInTheSameOrder_WhenPublishMessagesConcurrently()
    {
        // Arrange
        int publishThreadCount = 100;
        int publishesPerThread = 1000;
        int requeueMessageCount = 10000;
        
        int expectedCount = publishThreadCount * publishesPerThread + requeueMessageCount;
        
        ConcurrentQueue<Guid> walMessageIds = [];
        ConcurrentQueue<Guid> queueMessageIds = [];

        _walMock.Setup(w => w.Append(It.IsAny<EnqueueWalEvent>()))
            .Callback<WalEvent>(evt => walMessageIds.Enqueue((evt as EnqueueWalEvent)!.MessageId));
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Callback<Message>(message => queueMessageIds.Enqueue(message.Id))
            .Returns(true);
        
        Message[] messages = Enumerable
            .Range(0, requeueMessageCount)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 2, _timeProvider))
            .ToArray();
            
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns(messages);
        
        BrokerEngine sut = CreateSut(expiredPolicy: expiredPolicy);

        Task requeue = Task.Run(() =>
        {
            sut.Requeue();
        });

        Task publish = Task.Run(() =>
        {
            Parallel.For(0, publishThreadCount, _ =>
            {
                for (int i = 0; i < publishesPerThread; i++)
                {
                    sut.Publish([0x01, 0x02]);
                }
            });
        });

        // Act
        await Task.WhenAll(requeue, publish);
        
        // Assert
        walMessageIds.Count.ShouldBe(expectedCount);
        queueMessageIds.Count.ShouldBe(expectedCount);
        walMessageIds.ShouldBe(queueMessageIds, ignoreOrder: false);
    }

    private BrokerEngine CreateSut(
        IMessageQueue? queue = null,
        IWriteAheadLog? wal =  null,
        IExpiredMessagePolicy? expiredPolicy = null,
        TimeProvider? timeProvider = null,
        int maxPayloadLength = 5, 
        int maxDeliveryAttempts = 5)
    {
        return new(
            queue ?? _queueMock.Object,
            wal ?? _walMock.Object,
            expiredPolicy ?? _expiredPolicyMock.Object,
            timeProvider ?? _timeProvider,
            maxPayloadLength, 
            maxDeliveryAttempts);
    }
}