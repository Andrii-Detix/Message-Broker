using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;
using MessageBroker.Core.Queues;
using Microsoft.Extensions.Time.Testing;
using Shouldly;

namespace MessageBroker.UnitTests.Core.Queues;

public class MessageQueueFactoryTests
{
    [Fact]
    public void Constructor_ThrowsException_WhenConfigIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new MessageQueueFactory(null!, timeProvider);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        MessageQueueOptions config = new();
        
        // Act
        Action actual = () => new MessageQueueFactory(config, null!);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_CreatesMessageQueueFactory_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        MessageQueueOptions config = new();
        
        // Act
        MessageQueueFactory actual = new(config, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Create_CreatesNewMessageQueue()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        MessageQueueOptions config = new();
        MessageQueueFactory sut = new(config, timeProvider);
        
        // Act
        IMessageQueue actual = sut.Create();
        
        // Assert
        actual.ShouldNotBeNull();
    }
}