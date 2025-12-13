using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Services.GarbageCollector;
using MessageBroker.Persistence.Services.GarbageCollector.Exceptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.GarbageCollector;

public class WalGarbageCollectorBackgroundServiceTests
{
    private readonly Mock<IWalGarbageCollectorService> _gcService;
    private readonly Mock<ILogger<WalGarbageCollectorBackgroundService>> _loggerMock;

    public WalGarbageCollectorBackgroundServiceTests()
    {
        _gcService = new();
        _loggerMock = new();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenGarbageCollectorServiceIsNull()
    {
        // Arrange
        GarbageCollectorOptions options = new()
        {
            CollectInterval = TimeSpan.FromMilliseconds(150),
        };
        ILogger<WalGarbageCollectorBackgroundService> logger = _loggerMock.Object;
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new WalGarbageCollectorBackgroundService(null!, options, timeProvider, logger);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        GarbageCollectorOptions options = new()
        {
            CollectInterval = TimeSpan.FromMilliseconds(150),
        };
        IWalGarbageCollectorService gcService = _gcService.Object;
        ILogger<WalGarbageCollectorBackgroundService> logger = _loggerMock.Object;
        
        // Act
        Action actual = () => new WalGarbageCollectorBackgroundService(gcService, options, null!, logger);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_ThrowsException_WhenCollectIntervalIsZeroOrNegative(int collectInterval)
    {
        // Arrange
        GarbageCollectorOptions options = new()
        {
            CollectInterval = TimeSpan.FromMilliseconds(collectInterval),
        };
        IWalGarbageCollectorService gcService = _gcService.Object;
        ILogger<WalGarbageCollectorBackgroundService> logger = _loggerMock.Object;
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new WalGarbageCollectorBackgroundService(gcService, options, timeProvider, logger);
        
        // Assert
        actual.ShouldThrow<CollectIntervalInvalidException>();
    }

    [Fact]
    public void Constructor_CreatesRequeueBackgroundService_WhenInputDataIsValid()
    {
        // Arrange
        GarbageCollectorOptions options = new()
        {
            CollectInterval = TimeSpan.FromMilliseconds(150),
        };
        IWalGarbageCollectorService gcService = _gcService.Object;
        ILogger<WalGarbageCollectorBackgroundService> logger = _loggerMock.Object;
        FakeTimeProvider timeProvider = new();
        
        // Act
        using WalGarbageCollectorBackgroundService sut = new(gcService, options, timeProvider, logger);
        
        // Assert
        sut.ShouldNotBeNull();
    }
    
    [Fact]
    public async Task ExecuteAsync_CallsGarbageCollectorServicePeriodically_WhenRunning()
    {
        // Arrange
        GarbageCollectorOptions options = new()
        {
            CollectInterval = TimeSpan.FromMilliseconds(10),
        };
        IWalGarbageCollectorService gcService = _gcService.Object;
        ILogger<WalGarbageCollectorBackgroundService> logger = _loggerMock.Object;
        FakeTimeProvider timeProvider = new();
        
        using WalGarbageCollectorBackgroundService sut = new(gcService, options, timeProvider, logger);
        
        // Act
        await sut.StartAsync(CancellationToken.None);
        
        for (int i = 0; i < 3; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(10.1));
            await Task.Delay(500);
        }
        
        await sut.StopAsync(CancellationToken.None);
        
        // Assert
        _gcService.Verify(r => r.Collect(), Times.AtLeast(2));
    }
    
    [Fact]
    public async Task ExecuteAsync_CallsGarbageCollectorServicePeriodically_WhenExceptionIsThrown()
    {
        // Arrange
        _gcService.Setup(r => r.Collect())
            .Throws(new Exception());
        
        GarbageCollectorOptions options = new()
        {
            CollectInterval = TimeSpan.FromMilliseconds(10),
        };
        IWalGarbageCollectorService gcService = _gcService.Object;
        ILogger<WalGarbageCollectorBackgroundService> logger = _loggerMock.Object;
        FakeTimeProvider timeProvider = new();
        
        using WalGarbageCollectorBackgroundService sut = new(gcService, options, timeProvider, logger);
        
        // Act
        await sut.StartAsync(CancellationToken.None);
        
        for (int i = 0; i < 3; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(10.1));
            await Task.Delay(500);
        }
        
        await sut.StopAsync(CancellationToken.None);
        
        // Assert
        _gcService.Verify(r => r.Collect(), Times.AtLeast(2));
    }
}