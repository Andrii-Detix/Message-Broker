using MessageBroker.Core.Messages.Exceptions;
using MessageBroker.Core.Messages.Models;
using Shouldly;

namespace MessageBroker.UnitTests.Core.Messages;

public class MessageTests
{
    [Fact]
    public void Create_ThrowsException_WhenPayloadIsNull()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        byte[] payload = null!;
        int maxDeliveryAttempts = 1;
        
        // Act
        Action actual = () => Message.Create(id, payload, maxDeliveryAttempts);

        // Assert
        actual.ShouldThrow<PayloadNullReferenceException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Create_ThrowsException_WhenMaxDeliveryAttemptsIsLessThanOne(int maxDeliveryAttempts)
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        
        // Act
        Action actual = () => Message.Create(id, payload, maxDeliveryAttempts);

        // Assert
        actual.ShouldThrow<MaxDeliveryAttemptsInvalidException>();
    }

    [Fact]
    public void Create_ReturnsMessage_WhenProvidedDataIsValid()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        int maxDeliveryAttempts = 1;
        
        // Act
        Message actual = Message.Create(id, payload, maxDeliveryAttempts);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.Id.ShouldBe(id);
        actual.Payload.ShouldBe(payload);
        actual.State.ShouldBe(MessageState.Created);
        actual.CreatedAt.ShouldBeLessThanOrEqualTo(DateTimeOffset.UtcNow);
        actual.CreatedAt.ShouldBeGreaterThanOrEqualTo(DateTimeOffset.UtcNow - TimeSpan.FromSeconds(1));
        actual.LastSentAt.ShouldBeNull();
        actual.DeliveryCount.ShouldBe(0);
        actual.MaxDeliveryAttempts.ShouldBe(maxDeliveryAttempts);
    }

    [Fact]
    public void TryEnqueue_TransitionsToEnqueued_WhenMessageIsCreated()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        
        // Act
        bool actual = sut.TryEnqueue();
        
        // Assert
        actual.ShouldBeTrue();
        sut.State.ShouldBe(MessageState.Enqueued);
    }

    [Fact]
    public void TryEnqueue_TransitionsToEnqueued_WhenMessageIsSentAndMaxDeliveryAttemptsIsNotReached()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 2);
        sut.TryEnqueue();
        sut.TrySend();
        
        // Act
        bool actual = sut.TryEnqueue();
        
        // Assert
        actual.ShouldBeTrue();
        sut.State.ShouldBe(MessageState.Enqueued);
    }

    [Fact]
    public void TryEnqueue_ReturnsFalse_WhenMessageIsAlreadyEnqueued()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        sut.TryEnqueue();
        
        // Act
        bool actual = sut.TryEnqueue();
        
        // Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void TryEnqueue_TransitionsToFailed_WhenMaxDeliveryAttemptsIsReached()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        sut.TryEnqueue();
        sut.TrySend();
        
        // Act
        bool actual = sut.TryEnqueue();
        
        // Assert
        actual.ShouldBeFalse();
        sut.State.ShouldBe(MessageState.Failed);
    }

    [Fact]
    public void TrySend_TransitionsToSent_WhenMessageIsEnqueued()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        sut.TryEnqueue();
        
        // Act
        bool actual = sut.TrySend();
        
        // Assert
        actual.ShouldBeTrue();
        sut.State.ShouldBe(MessageState.Sent);
        sut.LastSentAt.ShouldNotBeNull();
        sut.LastSentAt.Value.ShouldBeLessThanOrEqualTo(DateTimeOffset.UtcNow);
        sut.LastSentAt.Value.ShouldBeGreaterThanOrEqualTo(DateTimeOffset.UtcNow - TimeSpan.FromSeconds(1));
        sut.DeliveryCount.ShouldBe(1);
    }

    [Fact]
    public void TrySend_ReturnsFalse_WhenMessageIsNotEnqueued()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        
        // Act
        bool actual = sut.TrySend();
        
        // Assert
        actual.ShouldBeFalse();
        sut.State.ShouldBe(MessageState.Created);
        sut.LastSentAt.ShouldBeNull();
        sut.DeliveryCount.ShouldBe(0);
    }

    [Fact]
    public void TryMarkDelivered_TransitionsToDelivered_WhenMessageIsSent()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        sut.TryEnqueue();
        sut.TrySend();
        
        // Act
        bool actual = sut.TryMarkDelivered();
        
        // Assert
        actual.ShouldBeTrue();
        sut.State.ShouldBe(MessageState.Delivered);
    }

    [Fact]
    public void TryMarkDelivered_ReturnsFalse_WhenMessageIsNotSent()
    {
        // Arrange
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1);
        sut.TryEnqueue();
        
        // Act
        bool actual = sut.TryMarkDelivered();
        
        // Assert
        actual.ShouldBeFalse();
        sut.State.ShouldBe(MessageState.Enqueued);
    }
}