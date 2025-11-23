using System.Collections.Concurrent;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Core.Queues;
using MessageBroker.Core.Queues.Exceptions;
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
        Action actual = () => new MessageQueue(maxSwapCount);
        
        // Assert
        actual.ShouldThrow<MaxSwapCountInvalidException>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void Constructor_CreateQueue_WhenMaxSwapCountIsEqualOrGreaterThanOne(int maxSwapCount)
    {
        // Act
        MessageQueue actual = new(maxSwapCount);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.Count.ShouldBe(0);
    }
    
    [Fact]
    public void TryEnqueue_AddsMessageToQueue_WhenMessageIsNewAndUnique()
    {
        // Arrange
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
        
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
        Message message1 = Message.Create(id, [], 1);
        Message message2 = Message.Create(id, [], 1);
        MessageQueue sut = new(1);
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        message.TryEnqueue();
        message.TrySend();
        message.TryMarkDelivered();
        MessageQueue sut = new(1);
        
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
        Message deliveredMessage = Message.Create(id, [], 1);
        deliveredMessage.TryEnqueue();
        deliveredMessage.TrySend();
        deliveredMessage.TryMarkDelivered();
        Message createdMessage = Message.Create(id, [], 1);
        MessageQueue sut = new(1);
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
        Message message1 = Message.Create(id, [], 1);
        MessageQueue sut = new(1);
        sut.TryEnqueue(message1);
        sut.TryConsume(out _);
        sut.Ack(id);
        Message message2 = Message.Create(id, [], 1);
        
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
        MessageQueue sut = new(1);
        int messageCount = 10000;
        
        Message[] messages = Enumerable
            .Range(0, messageCount)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1))
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
        MessageQueue sut = new(1);
        int uniqueMessageCount = 10000;
        
        Message[] messages = Enumerable
            .Range(0, uniqueMessageCount)
            .Select(_ => Message.Create(id, [], 1))
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 2);
        MessageQueue sut = new(1);
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        MessageQueue sut = new(1);
        
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        Message message1 = Message.Create(Guid.CreateVersion7(), [], 1);
        Message message2 = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        MessageQueue sut = new(100);
        
        Message[] messages = Enumerable
            .Range(0, 10000)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1))
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        MessageQueue sut = new(1);
        
        // Act
        Message? actual = sut.Ack(Guid.CreateVersion7());
        
        // Assert
        actual.ShouldBeNull();
    }
    
    [Fact]
    public void Ack_ReturnsNull_WhenMessageIsAlreadyAcknowledged()
    {
        // Arrange
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new(1);
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
        MessageQueue sut = new(1);
        
        Message[] messages = Enumerable.Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1))
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
        MessageQueue sut = new(1);

        Message[] messages = Enumerable.Range(0, 100)
            .Select(_ => Message.Create(Guid.CreateVersion7(), [], 1))
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
        MessageQueue sut = new(1);
        
        Guid[] expiredIds = [Guid.CreateVersion7(), Guid.CreateVersion7()];

        Message[] messages = 
        [
            Message.Create(Guid.CreateVersion7(), [], 1),
            Message.Create(expiredIds[0], [], 1),
            Message.Create(expiredIds[1], [], 1),
            Message.Create(Guid.CreateVersion7(), [], 1),
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