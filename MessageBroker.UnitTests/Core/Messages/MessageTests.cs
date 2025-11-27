using MessageBroker.Core.Messages.Exceptions;
using MessageBroker.Core.Messages.Models;
using Microsoft.Extensions.Time.Testing;
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        
        // Act
        Action actual = () => Message.Create(id, payload, maxDeliveryAttempts, timeProvider);

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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        
        // Act
        Action actual = () => Message.Create(id, payload, maxDeliveryAttempts, timeProvider);

        // Assert
        actual.ShouldThrow<MaxDeliveryAttemptsInvalidException>();
    }

    [Fact]
    public void Create_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        int maxDeliveryAttempts = 1;
        
        // Act
        Action actual = () => Message.Create(id, payload, maxDeliveryAttempts, null!);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Create_ReturnsMessage_WhenProvidedDataIsValid()
    {
        // Arrange
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        int maxDeliveryAttempts = 1;
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        
        // Act
        Message actual = Message.Create(id, payload, maxDeliveryAttempts, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.Id.ShouldBe(id);
        actual.Payload.ShouldBe(payload);
        actual.State.ShouldBe(MessageState.Created);
        actual.CreatedAt.ShouldBe(timeProvider.GetUtcNow());
        actual.LastSentAt.ShouldBeNull();
        actual.DeliveryCount.ShouldBe(0);
        actual.MaxDeliveryAttempts.ShouldBe(maxDeliveryAttempts);
    }

    [Fact]
    public void TryEnqueue_TransitionsToEnqueued_WhenMessageIsCreated()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 2, timeProvider);
        sut.TryEnqueue();
        sut.TrySend(timeProvider);
        
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        sut.TryEnqueue();
        sut.TrySend(timeProvider);
        
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        sut.TryEnqueue();
        
        // Act
        bool actual = sut.TrySend(timeProvider);
        
        // Assert
        actual.ShouldBeTrue();
        sut.State.ShouldBe(MessageState.Sent);
        sut.LastSentAt.ShouldNotBeNull();
        sut.LastSentAt.Value.ShouldBe(timeProvider.GetUtcNow());
        sut.DeliveryCount.ShouldBe(1);
    }

    [Fact]
    public void TrySend_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        
        // Act
        Action actual = () => sut.TrySend(null!);
        
        //  Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void TrySend_ReturnsFalse_WhenMessageIsNotEnqueued()
    {
        // Arrange
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        
        // Act
        bool actual = sut.TrySend(timeProvider);
        
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        sut.TryEnqueue();
        sut.TrySend(timeProvider);
        
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
        FakeTimeProvider timeProvider = new FakeTimeProvider();
        Message sut = Message.Create(Guid.CreateVersion7(), [], 1, timeProvider);
        sut.TryEnqueue();
        
        // Act
        bool actual = sut.TryMarkDelivered();
        
        // Assert
        actual.ShouldBeFalse();
        sut.State.ShouldBe(MessageState.Enqueued);
    }
}