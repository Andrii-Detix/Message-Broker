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
}