using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.RequeueServices;
using MessageBroker.Engine.RequeueServices.Exceptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.RequeueServices;

public class RequeueBackgroundServiceTests
{
    private readonly Mock<IRequeueService> _requeueServiceMock;
    private readonly Mock<ILogger<RequeueBackgroundService>> _loggerMock;

    public RequeueBackgroundServiceTests()
    {
        _requeueServiceMock = new();
        _loggerMock = new();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenRequeueServiceIsNull()
    {
        // Arrange
        TimeSpan checkInterval = TimeSpan.FromMicroseconds(150);
        FakeTimeProvider timeProvider = new();
        ILogger<RequeueBackgroundService> logger = _loggerMock.Object;
        
        // Act
        Action actual = () => new RequeueBackgroundService(null, checkInterval, timeProvider, logger);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_ThrowsException_WhenCheckIntervalIsZeroOrNegative(int checkIntervalInMilliseconds)
    {
        // Arrange
        TimeSpan checkInterval = TimeSpan.FromMilliseconds(checkIntervalInMilliseconds);
        IRequeueService requeueService = _requeueServiceMock.Object;
        FakeTimeProvider timeProvider = new();
        ILogger<RequeueBackgroundService> logger = _loggerMock.Object;
        
        // Act
        Action actual = () => new RequeueBackgroundService(requeueService, checkInterval, timeProvider, logger);
        
        // Assert
        actual.ShouldThrow<CheckIntervalInvalidException>();
    }

    [Fact]
    public void Constructor_CreatesRequeueBackgroundService_WhenInputDataIsValid()
    {
        // Arrange
        TimeSpan checkInterval = TimeSpan.FromMicroseconds(150);
        IRequeueService requeueService = _requeueServiceMock.Object;
        FakeTimeProvider timeProvider = new();
        ILogger<RequeueBackgroundService> logger = _loggerMock.Object;
        
        // Act
        RequeueBackgroundService actual = new(requeueService, checkInterval, timeProvider, logger);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Constructor_CreatesRequeueBackgroundService_WhenLoggerIsNotProvided()
    {
        // Arrange
        TimeSpan checkInterval = TimeSpan.FromMilliseconds(150);
        IRequeueService requeueService = _requeueServiceMock.Object;
        FakeTimeProvider timeProvider = new();
        
        // Act
        RequeueBackgroundService actual = new(requeueService, checkInterval, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public async Task ExecuteAsync_CallsRequeueServicePeriodically_WhenRunning()
    { 
        // Arrange
        TimeSpan checkInterval = TimeSpan.FromSeconds(10);
        IRequeueService requeueService = _requeueServiceMock.Object;
        FakeTimeProvider timeProvider = new();
    
        using RequeueBackgroundService sut = new(requeueService, checkInterval, timeProvider);
    
        // Act
        await sut.StartAsync(CancellationToken.None);

        for (int i = 0; i < 3; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(10.1));
            await Task.Delay(500);
        }
    
        await sut.StopAsync(CancellationToken.None);
    
        // Assert
        _requeueServiceMock.Verify(r => r.Requeue(), Times.AtLeast(2));
    }

    [Fact]
    public async Task ExecuteAsync_CallsRequeueServicePeriodically_WhenExceptionIsThrown()
    {
        // Arrange
        _requeueServiceMock.Setup(r => r.Requeue())
            .Throws(new Exception());
        
        TimeSpan checkInterval = TimeSpan.FromMilliseconds(10);
        IRequeueService requeueService = _requeueServiceMock.Object;
        FakeTimeProvider timeProvider = new();
        using RequeueBackgroundService sut = new(requeueService, checkInterval, timeProvider);
        
        // Act
        await sut.StartAsync(CancellationToken.None);
        
        for (int i = 0; i < 3; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(10.1));
            await Task.Delay(500);
        }
        
        await sut.StopAsync(CancellationToken.None);
        
        // Assert
        _requeueServiceMock.Verify(r => r.Requeue(), Times.AtLeast(2));
    }
}