using MessageBroker.Core.Messages.Models;
using MessageBroker.Core.Queues;
using Shouldly;

namespace MessageBroker.UnitTests.Core.Queues;

public class MessageQueueTests
{
    [Fact]
    public void TryEnqueue_AddsMessageToQueue_WhenMessageIsNewAndUnique()
    {
        // Arrange
        Message message = Message.Create(Guid.CreateVersion7(), [], 1);
        MessageQueue sut = new();
        
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
        MessageQueue sut = new();
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
        MessageQueue sut = new();
        
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
        MessageQueue sut = new();
        sut.TryEnqueue(deliveredMessage);
        
        // Act
        bool actual = sut.TryEnqueue(createdMessage);
        
        // Assert
        actual.ShouldBeTrue();
        sut.Count.ShouldBe(1);
    }

    [Fact]
    public void TryEnqueue_AddsAllMessages_WhenTryEnqueueMessagesConcurrently()
    {
        // Arrange
        MessageQueue sut = new();
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
        MessageQueue sut = new();
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
}