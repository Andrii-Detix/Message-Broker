using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Core.Queues;
using MessageBroker.Core.Queues.Exceptions;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Core.Queues;

public class MessageQueueTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_ThrowsException_WhenMaxSwapCountIsLessThanOne(int maxSwapCount)
    {
        // Act
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Action actual = () => new MessageQueue(maxSwapCount, timeProvider);
        
        // Assert
        actual.ShouldThrow<MaxSwapCountInvalidException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Act
        Action actual = () => new MessageQueue(1, null!);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void Constructor_CreateQueue_WhenMaxSwapCountIsEqualOrGreaterThanOne(int maxSwapCount)
    {
        // Act
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue actual = new(maxSwapCount, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.Count.ShouldBe(0);
    }
    
    [Fact]
    public void TryEnqueue_AddsMessageToQueue_WhenMessageIsNewAndUnique()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        
        // Act
        bool actual = sut.TryEnqueue(message);
        
        // Assert
        actual.ShouldBeTrue();
        sut.Count.ShouldBe(1);
    }

    [Fact]
    public void TryEnqueue_ReturnsFalse_WhenMessageWithIdAlreadyExists()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message1 = Message.Create(id, [], 1, timeProvider);
        Message message2 = Message.Create(id, [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message1);
        
        // Act
        bool actual = sut.TryEnqueue(message2);
        
        // Assert
        actual.ShouldBeFalse();
        sut.Count.ShouldBe(1);
    }

    [Fact]
    public void TryEnqueue_ReturnsFalse_WhenMessageCannotBeEnqueued()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        message.TryEnqueue();
        message.TrySend(timeProvider);
        message.TryMarkDelivered();
        MessageQueue sut = new(1, timeProvider);
        
        // Act
        bool actual = sut.TryEnqueue(message);
        
        // Assert
        actual.ShouldBeFalse();
        sut.Count.ShouldBe(0);
    }

    [Fact]
    public void TryEnqueue_AddsMessageToQueue_WhenTryEnqueueValidMessageAfterOneWithSameIdThatCannotBeEnqueued()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message deliveredMessage = Message.Create(id, [], 1, timeProvider);
        deliveredMessage.TryEnqueue();
        deliveredMessage.TrySend(timeProvider);
        deliveredMessage.TryMarkDelivered();
        Message createdMessage = Message.Create(id, [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(deliveredMessage);
        
        // Act
        bool actual = sut.TryEnqueue(createdMessage);
        
        // Assert
        actual.ShouldBeTrue();
        sut.Count.ShouldBe(1);
    }

    [Fact]
    public void TryEnqueue_AddsMessageWithSameId_WhenPreviousMessageWithSameIdWasAcknowledged()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message1 = Message.Create(id, [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message1);
        sut.TryConsume(out _);
        sut.Ack(id);
        Message message2 = Message.Create(id, [], 1, timeProvider);
        
        // Act
        bool actual = sut.TryEnqueue(message2);
        
        // Assert
        actual.ShouldBeTrue();
        message2.State.ShouldBe(MessageState.Enqueued);
        sut.Count.ShouldBe(1);
    }

    [Fact]
    public void TryEnqueue_AddsAllMessages_WhenTryEnqueueMessagesConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);
        int messageCount = 10000;
        
        Message[] messages = Enumerable
            .Range(0, messageCount)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1, timeProvider))
            .ToArray();
        
        // Act
        Parallel.ForEach(messages, message =>
        {
            sut.TryEnqueue(message);
        });
        
        // Assert
        sut.Count.ShouldBe(messageCount);
    }

    [Fact]
    public void TryEnqueue_AddsOnlyUniqueMessage_WhenTryEnqueueMessagesConcurrentlyWithSameId()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);
        int uniqueMessageCount = 10000;
        
        Message[] messages = Enumerable
            .Range(0, uniqueMessageCount)
            .Select(_ => Message.Create(id, [], 1, timeProvider))
            .ToArray();
        
        messages = messages.Concat(messages).ToArray();
        
        // Act
        Parallel.ForEach(messages, message =>
        {
            sut.TryEnqueue(message);
        });
        
        // Assert
        sut.Count.ShouldBe(1);
    }

    [Fact]
    public void TryEnqueue_RequeueExpiredMessage_WhenMessageDoesNotReachMaxDeliveryAttempts()
    {
        // Arrange 
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 2, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        sut.TryConsume(out _);
        
        Mock<IExpiredMessagePolicy> policyMock = new();
        policyMock.Setup(p => p.IsExpired(It.IsAny<Message>())).Returns(true);
        IExpiredMessagePolicy policy = policyMock.Object;
        
        Message expired = sut.TakeExpiredMessages(policy).First();
        
        // Act
        bool actual = sut.TryEnqueue(expired);
        
        // Assert
        actual.ShouldBeTrue();
        expired.State.ShouldBe(MessageState.Enqueued);
        sut.Count.ShouldBe(1);
    }
    
    [Fact]
    public void TryEnqueue_TransitionsMessageToFailed_WhenExpiredMessageReachesMaxDeliveryAttempts()
    {
        // Arrange 
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        sut.TryConsume(out _);
        
        Mock<IExpiredMessagePolicy> policyMock = new();
        policyMock.Setup(p => p.IsExpired(It.IsAny<Message>())).Returns(true);
        IExpiredMessagePolicy policy = policyMock.Object;
        
        Message expired = sut.TakeExpiredMessages(policy).First();
        
        // Act
        bool actual = sut.TryEnqueue(expired);
        
        // Assert
        actual.ShouldBeFalse();
        expired.State.ShouldBe(MessageState.Failed);
        sut.Count.ShouldBe(0);
    }

    [Fact]
    public void TryConsume_ReturnsFalse_WhenQueueIsEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);
        
        // Act
        bool success = sut.TryConsume(out Message? actual);
        
        // Assert
        success.ShouldBeFalse();
        actual.ShouldBeNull();
        sut.Count.ShouldBe(0);
    }

    [Fact]
    public void TryConsume_ReturnsMessage_WhenQueueIsNotEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        
        // Act
        bool success = sut.TryConsume(out Message? actual);
        
        // Assert
        success.ShouldBeTrue();
        actual.ShouldNotBeNull();
        actual.ShouldBe(message);
        sut.Count.ShouldBe(0);
    }

    [Fact]
    public void TryConsume_TransitionsMessageStateToSent_WhenMessageIsConsumed()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        
        // Act
        bool success = sut.TryConsume(out Message? actual);
        
        // Assert
        success.ShouldBeTrue();
        actual.ShouldNotBeNull();
        actual.State.ShouldBe(MessageState.Sent);
    }
    
    [Fact]
    public void TryConsume_ReturnsMessageInEnqueueOrder_WhenQueueHasMoreThanOneMessages()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message1 = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        Message message2 = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message1);
        sut.TryEnqueue(message2);
        
        // Act
        sut.TryConsume(out Message? actual1);
        sut.TryConsume(out Message? actual2);
        
        // Assert
        actual1.ShouldNotBeNull();
        actual2.ShouldNotBeNull();
        actual1.ShouldBe(message1);
        actual2.ShouldBe(message2);
        sut.Count.ShouldBe(0);
    }

    [Fact]
    public void TryConsume_ReturnsMessagesInEnqueueOrderForEachThread_WhenTryConsumeMessagesConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(100, timeProvider);
        
        Message[] messages = Enumerable
            .Range(0, 10000)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1, timeProvider))
            .ToArray();
        
        Array.ForEach(messages, message => sut.TryEnqueue(message));

        List<Message>[] threadMessages = [[], []];
        
        // Act
        Parallel.ForEach(Enumerable.Range(0, 2), threadIdx =>
        {
            for (int i = 0; i < 5000; i++)
            {
                sut.TryConsume(out Message? message);
                threadMessages[threadIdx].Add(message!);
            }
        });
        
        // Assert
        sut.Count.ShouldBe(0);
        threadMessages.Sum(tm => tm.Count).ShouldBe(10000);
        foreach (var actual in threadMessages)
        {
            int[] messageIndexes = actual
                .Select(message => messages.IndexOf(message))
                .ToArray();
            
            messageIndexes.ShouldNotContain(-1);
            messageIndexes.ShouldBeInOrder(SortDirection.Ascending);
        }
    }

    [Fact]
    public void Ack_ReturnsMessage_WhenMessageIsSent()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        sut.TryConsume(out _);
        
        // Act
        Message? actual = sut.Ack(message.Id);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBe(message);
    }

    [Fact]
    public void Ack_TransitionsMessageStateToDelivered_WhenMessageIsAcknowledged()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        sut.TryConsume(out _);
        
        // Act
        Message? actual = sut.Ack(message.Id);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.State.ShouldBe(MessageState.Delivered);
    }
    
    [Fact]
    public void Ack_ReturnsNull_WhenMessageIsNotSent()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        
        // Act
        Message? actual = sut.Ack(message.Id);
        
        // Assert
        actual.ShouldBeNull();
    }
    
    [Fact]
    public void Ack_ReturnsNull_WhenMessageDoesNotContainMessageWithId()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);
        
        // Act
        Message? actual = sut.Ack(Guid.CreateVersion7());
        
        // Assert
        actual.ShouldBeNull();
    }
    
    [Fact]
    public void Ack_ReturnsNull_WhenMessageIsAlreadyAcknowledged()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        sut.TryConsume(out _);
        sut.Ack(message.Id);
        
        // Act
        Message? actual = sut.Ack(message.Id);
        
        // Assert
        actual.ShouldBeNull();
    }

    [Fact]
    public void Ack_AcknowledgesMessageOnlyOnce_WhenTryAcknowledgeSameMessageConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message message = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        MessageQueue sut = new(1, timeProvider);
        sut.TryEnqueue(message);
        sut.TryConsume(out _);
        ConcurrentBag<Message?> results = [];
        
        // Act
        Parallel.ForEach(Enumerable.Range(0, 1000), _ =>
        {
            Message? actual = sut.Ack(message.Id);
            results.Add(actual);
        });
        
        // Assert
        results.Count(m => m is not null).ShouldBe(1);
        results.First(m => m is not null).ShouldBe(message);
    }

    [Fact]
    public void TakeExpiredMessages_ReturnsEmptyIEnumerable_WhenAllMessagesAreNotSent()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);
        
        Message[] messages = Enumerable.Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1, timeProvider))
            .ToArray();
        
        Array.ForEach(messages, message => sut.TryEnqueue(message));
        
        Mock<IExpiredMessagePolicy> policyMock = new();
        policyMock.Setup(p => p.IsExpired(It.IsAny<Message>())).Returns(true);
        IExpiredMessagePolicy policy = policyMock.Object;
        
        // Act
        IEnumerable<Message> actual = sut.TakeExpiredMessages(policy).ToList();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBeEmpty();
    }
    
    [Fact]
    public void TakeExpiredMessages_ReturnsEmptyIEnumerable_WhenAllMessagesAreNotExpired()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);

        Message[] messages = Enumerable.Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1, timeProvider))
            .ToArray();
        
        foreach (Message message in messages)
        {
            sut.TryEnqueue(message);
            sut.TryConsume(out _);
        }

        Mock<IExpiredMessagePolicy> policyMock = new();
        policyMock.Setup(p => p.IsExpired(It.IsAny<Message>())).Returns(false);
        IExpiredMessagePolicy policy = policyMock.Object;
        
        // Act
        IEnumerable<Message> actual = sut.TakeExpiredMessages(policy).ToList();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBeEmpty();
    }

    [Fact]
    public void TakeExpiredMessages_ReturnsOnlyExpiredMessages_WhenQueueContainsMixedMessages()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        MessageQueue sut = new(1, timeProvider);
        
        Guid[] expiredIds = [Guid.CreateVersion7(), Guid.CreateVersion7()];

        Message[] messages = 
        [
            Message.Create(Guid.CreateVersion7(), [], 1, timeProvider),
            Message.Create(expiredIds[0], [], 1, timeProvider),
            Message.Create(expiredIds[1], [], 1, timeProvider),
            Message.Create(Guid.CreateVersion7(), [], 1, timeProvider),
        ];
        
        foreach (var message in messages)
        {
            sut.TryEnqueue(message);
            sut.TryConsume(out _);
        }

        Mock<IExpiredMessagePolicy> policyMock = new();
        policyMock.Setup(p => p.IsExpired(It.IsAny<Message>()))
            .Returns((Message message) => expiredIds.Contains(message.Id));
        IExpiredMessagePolicy policy = policyMock.Object;
        
        // Act
        IEnumerable<Message> actual = sut.TakeExpiredMessages(policy).ToList();
        
        // Assert
        actual.ShouldNotBeEmpty();
        actual.Count().ShouldBe(2);
        actual.Select(m => m.Id).ShouldBe(expiredIds, ignoreOrder: true);
    }
}