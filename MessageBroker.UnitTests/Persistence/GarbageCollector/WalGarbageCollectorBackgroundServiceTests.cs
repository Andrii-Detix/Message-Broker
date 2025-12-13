using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Services.GarbageCollector;
using MessageBroker.Persistence.Services.GarbageCollector.Exceptions;
using Microsoft.Extensions.Logging;
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
        
        // Act
        Action actual = () => new WalGarbageCollectorBackgroundService(null!, options, logger);
        
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
        
        // Act
        Action actual = () => new WalGarbageCollectorBackgroundService(gcService, options, logger);
        
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
        
        // Act
        using WalGarbageCollectorBackgroundService sut = new(gcService, options, logger);
        
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
        
        using WalGarbageCollectorBackgroundService sut = new(gcService, options, logger);
        
        // Act
        await sut.StartAsync(CancellationToken.None);
        
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        
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
        
        using WalGarbageCollectorBackgroundService sut = new(gcService, options, logger);
        
        // Act
        await sut.StartAsync(CancellationToken.None);
        
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        
        await sut.StopAsync(CancellationToken.None);
        
        // Assert
        _gcService.Verify(r => r.Collect(), Times.AtLeast(2));
    }
}