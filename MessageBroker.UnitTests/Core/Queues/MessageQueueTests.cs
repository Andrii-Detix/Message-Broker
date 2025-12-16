using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Core.Queues;
using MessageBroker.Core.Queues.Exceptions;
using MessageBroker.UnitTests.Helpers;
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
        Message message = MessageHelper.CreateMessage();
        MessageQueue sut = CreateSut();
        
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
        Message message1 = MessageHelper.CreateMessage(messageId: id, payload: []);
        Message message2 = MessageHelper.CreateMessage(messageId: id, payload: [0x01]);
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage(timeProvider: timeProvider);
        
        message.TryEnqueue();
        message.TrySend(timeProvider);
        message.TryMarkDelivered();

        MessageQueue sut = CreateSut();
        
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
        Message deliveredMessage = MessageHelper.CreateMessage(messageId: id);
        
        deliveredMessage.TryEnqueue();
        deliveredMessage.TrySend(timeProvider);
        deliveredMessage.TryMarkDelivered();
        
        Message createdMessage = MessageHelper.CreateMessage(messageId: id);
        
        MessageQueue sut = CreateSut();
        
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
        Message message1 = MessageHelper.CreateMessage(messageId: id);
        
        MessageQueue sut = CreateSut();
        
        sut.TryEnqueue(message1);
        sut.TryConsume(out _);
        sut.Ack(id);
        
        Message message2 = MessageHelper.CreateMessage(messageId: id);
        
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
        MessageQueue sut = CreateSut();
        
        int messageCount = 10000;
        Message[] messages = MessageHelper.CreateMessages(messageCount);
        
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
        MessageQueue sut = CreateSut();
        
        Guid id = Guid.CreateVersion7();
        int uniqueMessageCount = 10000;
        Message[] messages = MessageHelper.CreateMessages(uniqueMessageCount, fixedId: id);
        
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
        Message message = MessageHelper.CreateMessage(maxDeliveryAttempts: 2);
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage(maxDeliveryAttempts: 1);
        
        MessageQueue sut = CreateSut();
        
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
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message message1 = MessageHelper.CreateMessage();
        Message message2 = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message[] messages = MessageHelper.CreateMessages(10000);
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        MessageQueue sut = CreateSut();
        
        // Act
        Message? actual = sut.Ack(Guid.CreateVersion7());
        
        // Assert
        actual.ShouldBeNull();
    }
    
    [Fact]
    public void Ack_ReturnsNull_WhenMessageIsAlreadyAcknowledged()
    {
        // Arrange
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message message = MessageHelper.CreateMessage();
        
        MessageQueue sut = CreateSut();
        
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
        Message[] messages = MessageHelper.CreateMessages(100);
        
        MessageQueue sut = CreateSut();
        
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
        MessageQueue sut = CreateSut();

        Message[] messages = MessageHelper.CreateMessages(100);
        
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
        MessageQueue sut = CreateSut();
        
        Guid[] expiredIds = [Guid.CreateVersion7(), Guid.CreateVersion7()];

        Message[] messages = 
        [
            MessageHelper.CreateMessage(),
            MessageHelper.CreateMessage(messageId: expiredIds[0]),
            MessageHelper.CreateMessage(messageId: expiredIds[1]),
            MessageHelper.CreateMessage()
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

    [Fact]
    public void TakeExpiredMessages_RemovesOnlyExpiredMessagesFromInFlight_WhenInFlightContainsMixedMessages()
    {
        // Arrange
        MessageQueue sut = CreateSut();

        Message[] expiredMessages = MessageHelper.CreateMessages(2);
        Message[] activeMessages = MessageHelper.CreateMessages(2);

        Message[] mixedMessages = 
        [
            activeMessages[0],
            expiredMessages[0],
            expiredMessages[1],
            activeMessages[1]
        ];
        
        foreach (Message message in mixedMessages)
        {
            sut.TryEnqueue(message);
            sut.TryConsume(out _);
        }
        
        Mock<IExpiredMessagePolicy> policyMock = new();
        policyMock.Setup(p => p.IsExpired(It.IsAny<Message>()))
            .Returns((Message message) => expiredMessages.Select(m => m.Id).Contains(message.Id));
        IExpiredMessagePolicy policy = policyMock.Object;
        
        // Act
        sut.TakeExpiredMessages(policy);
        
        // Assert
        foreach (Message expiredMessage in expiredMessages)
        {
            sut.Ack(expiredMessage.Id).ShouldBeNull();
        }

        foreach (Message activeMessage in activeMessages)
        {
            sut.Ack(activeMessage.Id).ShouldNotBeNull();
        }
    }

    private static MessageQueue CreateSut(
        int? maxSwapCount = null,
        TimeProvider? timeProvider = null)
    {
        return new MessageQueue(
            maxSwapCount ?? 5,
            timeProvider ?? new FakeTimeProvider());
    }
}