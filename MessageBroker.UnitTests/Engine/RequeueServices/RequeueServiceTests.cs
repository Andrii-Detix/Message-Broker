using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.RequeueServices;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.RequeueServices;

public class RequeueServiceTests
{
    private readonly Mock<IMessageQueue> _queueMock;
    private readonly Mock<IWriteAheadLog> _walMock;
    private readonly Mock<IExpiredMessagePolicy> _expiredPolicyMock;

    public RequeueServiceTests()
    {
        _queueMock = new();
        _walMock = new();
        _expiredPolicyMock = new();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenMessageQueueIsNull()
    {
        // Arrange
        IWriteAheadLog wal = _walMock.Object;
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        
        // Act
        Action actual = () => new RequeueService(null, wal, expiredPolicy);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenWalIsNull()
    {
        // Arrange
        IMessageQueue queue = _queueMock.Object;
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        
        // Act
        Action actual = () => new RequeueService(queue, null, expiredPolicy);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenExpiredPolicyIsNull()
    {
        // Arrange
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        
        // Act
        Action actual = () => new RequeueService(queue, wal, null);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_CreatesRequeueService_WhenInputDataIsValid()
    {
        // Arrange
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        
        // Act
        RequeueService actual = new(queue, wal, expiredPolicy);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Requeue_AppendsRequeueEventAndEnqueuesMessage_WhenEverythingSucceeds()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message[] messages = Enumerable
            .Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 2, timeProvider))
            .ToArray();
        
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns(messages);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(true);

        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        RequeueService sut = new(queue, wal, expiredPolicy);
        
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
        FakeTimeProvider timeProvider = new();
        Message message = Message.Create(Guid.CreateVersion7(), [], 2, timeProvider);
        
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns([message]);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns(false);
        
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        RequeueService sut = new(queue, wal, expiredPolicy);
        
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
        FakeTimeProvider timeProvider = new();
        Message[] messages = Enumerable
            .Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 2, timeProvider))
            .ToArray();
        Message[] rejectedMessages = messages.Skip(10).Take(10).ToArray();
        
        IExpiredMessagePolicy expiredPolicy = _expiredPolicyMock.Object;
        _queueMock.Setup(q => q.TakeExpiredMessages(expiredPolicy))
            .Returns(messages);
        _queueMock.Setup(q => q.TryEnqueue(It.IsAny<Message>()))
            .Returns((Message message) => !rejectedMessages.Contains(message));
        
        IMessageQueue queue = _queueMock.Object;
        IWriteAheadLog wal = _walMock.Object;
        RequeueService sut = new(queue, wal, expiredPolicy);
        
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
}