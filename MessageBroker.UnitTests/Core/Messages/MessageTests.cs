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
        FakeTimeProvider timeProvider = new();
        
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
        FakeTimeProvider timeProvider = new();
        
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
        FakeTimeProvider timeProvider = new();
        
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
    public void Restore_ThrowsException_WhenPayloadIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        MessageState state = MessageState.Restored;
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        int deliveryCount = 0;
        int maxDeliveryAttempts = 5;

        // Act
        Action actual = () => Message.Restore(
            id, null!, state, createdAt, null, deliveryCount, maxDeliveryAttempts);

        // Assert
        actual.ShouldThrow<PayloadNullReferenceException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Restore_ThrowsException_WhenMaxDeliveryAttemptsIsLessThanOne(int maxDeliveryAttempts)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        MessageState state = MessageState.Restored;
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        int deliveryCount = 0;
        
        // Act
        Action actual = () => Message.Restore(
            id, payload, state, createdAt, null, deliveryCount, maxDeliveryAttempts);

        // Assert
        actual.ShouldThrow<MaxDeliveryAttemptsInvalidException>();
    }
    
    [Fact]
    public void Restore_ThrowsException_WhenDeliveryCountIsNegative()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        MessageState state = MessageState.Restored;
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => Message.Restore(
            id, payload, state, createdAt, null, -1, maxDeliveryAttempts);

        // Assert
        actual.ShouldThrow<DeliveryCountInvalidException>();
    }
    
    [Theory]
    [InlineData(MessageState.Created)]
    [InlineData(MessageState.Enqueued)]
    [InlineData(MessageState.Sent)]
    public void Restore_ThrowsException_WhenStateIsInvalidForRestoration(MessageState invalidState)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        byte[] payload = [];
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        int deliveryCount = 0;
        int maxDeliveryAttempts = 5;
        
        // Act
        Action actual = () => Message.Restore(
            id, payload, invalidState, createdAt, null, deliveryCount, maxDeliveryAttempts);

        // Assert
        actual.ShouldThrow<InvalidRestoreMessageStateException>();
    }
    
    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    public void Restore_SetsStateToFailed_WhenDeliveryCountReachedMaxAttemptsAndStateIsNotDelivered(int deliveryCount)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        MessageState inputState = MessageState.Restored;
        byte[] payload = [];
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        int maxDeliveryAttempts = 5;

        // Act
        Message actual = Message.Restore(
            id, payload, inputState, createdAt, null, deliveryCount, maxDeliveryAttempts);

        // Assert
        actual.State.ShouldBe(MessageState.Failed);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    public void Restore_KeepsStateAsDelivered_WhenDeliveryCountReachedMaxAttemptsAndStateIsDelivered(int deliveryCount)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        MessageState inputState = MessageState.Delivered;
        byte[] payload = [];
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        int maxDeliveryAttempts = 5;

        // Act
        Message actual = Message.Restore(
            id, payload, inputState, createdAt, null, deliveryCount, maxDeliveryAttempts);

        // Assert
        actual.State.ShouldBe(MessageState.Delivered);
    }

    [Fact]
    public void Restore_CreateRestoredMessage_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Guid id = Guid.CreateVersion7();
        MessageState inputState = MessageState.Restored;
        byte[] payload = [0x01, 0x02];
        DateTimeOffset createdAt = timeProvider.GetUtcNow();
        DateTimeOffset lastSentAt = timeProvider.GetUtcNow().Add(TimeSpan.FromMinutes(1));
        int deliveryCount = 1;
        int maxDeliveryAttempts = 5;
        
        // Act
        Message actual = Message.Restore(
            id, payload, inputState, createdAt, lastSentAt, deliveryCount, maxDeliveryAttempts);
        
        // Assert
        actual.ShouldNotBeNull();
        actual.Id.ShouldBe(id);
        actual.Payload.ShouldBe(payload);
        actual.State.ShouldBe(MessageState.Restored);
        actual.CreatedAt.ShouldBe(createdAt);
        actual.LastSentAt.ShouldBe(lastSentAt);
        actual.DeliveryCount.ShouldBe(deliveryCount);
        actual.MaxDeliveryAttempts.ShouldBe(maxDeliveryAttempts);
    }
    
    [Fact]
    public void TryEnqueue_TransitionsToEnqueued_WhenMessageIsCreated()
    {
        // Arrange
        Message sut = CreateSut();
        
        // Act
        bool actual = sut.TryEnqueue();
        
        // Assert
        actual.ShouldBeTrue();
        sut.State.ShouldBe(MessageState.Enqueued);
    }

    [Fact]
    public void TryEnqueue_TransitionsToEnqueued_WhenMessageIsRestored()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message sut = Message.Restore(
            Guid.CreateVersion7(),
            [],
            MessageState.Restored,
            timeProvider.GetUtcNow(), 
            null, 
            0, 
            5);
        
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
        FakeTimeProvider timeProvider = new();
        Message sut = CreateSut(maxDeliveryAttempts: 2, timeProvider: timeProvider);
        
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
        Message sut = CreateSut();
        
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
        FakeTimeProvider timeProvider = new();
        Message sut = CreateSut(maxDeliveryAttempts: 1, timeProvider: timeProvider);
        
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
        FakeTimeProvider timeProvider = new();
        Message sut = CreateSut(timeProvider: timeProvider);
        
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
        Message sut = CreateSut();
        
        // Act
        Action actual = () => sut.TrySend(null!);
        
        //  Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void TrySend_ReturnsFalse_WhenMessageIsNotEnqueued()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message sut = CreateSut(timeProvider: timeProvider);
        
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
        FakeTimeProvider timeProvider = new();
        Message sut = CreateSut(timeProvider: timeProvider);
        
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
        FakeTimeProvider timeProvider = new();
        Message sut = CreateSut(timeProvider: timeProvider);
        
        sut.TryEnqueue();
        
        // Act
        bool actual = sut.TryMarkDelivered();
        
        // Assert
        actual.ShouldBeFalse();
        sut.State.ShouldBe(MessageState.Enqueued);
    }
    
    private Message CreateSut(
        Guid? messageId = null,
        byte[]? payload = null,
        int? maxDeliveryAttempts = null,
        TimeProvider? timeProvider = null)
    {
        return Message.Create(
            messageId ?? Guid.CreateVersion7(), 
            payload ?? [], 
            maxDeliveryAttempts ?? 5, 
            timeProvider ?? new FakeTimeProvider());
    }
}