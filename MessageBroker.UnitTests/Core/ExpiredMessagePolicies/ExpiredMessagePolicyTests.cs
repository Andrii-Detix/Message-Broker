using MessageBroker.Core.ExpiredMessagePolicies;
using MessageBroker.Core.ExpiredMessagePolicies.Exceptions;
using MessageBroker.Core.Messages.Models;
using MessageBroker.UnitTests.Helpers;
using Microsoft.Extensions.Time.Testing;
using Shouldly;

namespace MessageBroker.UnitTests.Core.ExpiredMessagePolicies;

public class ExpiredMessagePolicyTests
{
    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        TimeSpan expirationTime = TimeSpan.FromMinutes(5);

        // Act
        Action actual = () => new ExpiredMessagePolicy(expirationTime, null);

        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    public static IEnumerable<object[]> InvalidExpirationTimeDataSet =>
    [
        [TimeSpan.Zero],
        [TimeSpan.FromSeconds(-1)]
    ];
    
    [Theory]
    [MemberData(nameof(InvalidExpirationTimeDataSet))]
    public void Constructor_ThrowsException_WhenInputExpirationTimeIsZeroOrNegative(TimeSpan expirationTime)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new ExpiredMessagePolicy(expirationTime, timeProvider);
        
        // Assert
        actual.ShouldThrow<MessageExpirationTimeInvalidException>();
    }
    
    [Fact]
    public void Constructor_CreatesExpiredMessagePolicy_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        TimeSpan expirationTime = TimeSpan.FromMinutes(5);
        
        // Act
        ExpiredMessagePolicy actual = new(expirationTime, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
    }
    
    [Fact]
    public void IsExpired_ReturnsTrue_WhenMessageExpirationTimeHasPassed()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        TimeSpan expirationTime = TimeSpan.FromMinutes(5);
        ExpiredMessagePolicy sut = new(expirationTime, timeProvider);

        Message message = MessageHelper.CreateMessage();
        
        message.TryEnqueue();
        message.TrySend(timeProvider);
        
        timeProvider.Advance(TimeSpan.FromMinutes(5));
        
        // Act
        bool actual = sut.IsExpired(message);
        
        // Assert
        actual.ShouldBeTrue();
    }

    [Fact]
    public void IsExpired_ReturnsFalse_WhenMessageExpirationTimeHasNotPassed()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        TimeSpan expirationTime = TimeSpan.FromMinutes(5);
        ExpiredMessagePolicy sut = new(expirationTime, timeProvider);

        Message message = MessageHelper.CreateMessage();
        
        message.TryEnqueue();
        message.TrySend(timeProvider);
        
        timeProvider.Advance(TimeSpan.FromMinutes(4));
        
        // Act
        bool actual = sut.IsExpired(message);
        
        // Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void IsExpired_ReturnsFalse_WhenMessageIsNotInSentState()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        TimeSpan expirationTime = TimeSpan.FromMinutes(5);
        ExpiredMessagePolicy sut = new(expirationTime, timeProvider);

        Message message = MessageHelper.CreateMessage();
        
        // Act
        bool actual = sut.IsExpired(message);
        
        // Assert
        actual.ShouldBeFalse();
    }

    [Fact]
    public void IsExpired_ThrowsException_WhenMessageIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        TimeSpan expirationTime = TimeSpan.FromMinutes(5);
        ExpiredMessagePolicy sut = new(expirationTime, timeProvider);
        
        // Act
        Action actual = () => sut.IsExpired(null!);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
}