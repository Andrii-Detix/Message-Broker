using MessageBroker.Engine.Services.Shutdown;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace MessageBroker.UnitTests.Engine.ShutdownServices;

public class GracefulShutdownServiceTests
{
    private readonly Mock<IHostApplicationLifetime> _appLifetimeMock;
    private readonly Mock<ILogger<GracefulShutdownService>> _loggerMock;
    
    public GracefulShutdownServiceTests()
    {
        _appLifetimeMock = new();
        _loggerMock = new();
    }
    
    [Fact]
    public void Raise_StopsApplication_WhenCriticalErrorOccurs()
    {
        // Arrange
        Exception exception = new Exception("Custom exception");
        string message = "Critical Error";
        
        GracefulShutdownService sut = new(_appLifetimeMock.Object, _loggerMock.Object);

        // Act
        sut.Raise(message, exception);

        // Assert
        _appLifetimeMock.Verify(a => a.StopApplication(), Times.Once);
    }
    
    [Fact]
    public void Raise_LogsCriticalError_WhenCriticalErrorOccurs()
    {
        // Arrange
        Exception exception = new Exception("Custom exception");
        string message = "Critical Error";
        
        GracefulShutdownService sut = new(_appLifetimeMock.Object, _loggerMock.Object);
        
        // Act
        sut.Raise(message, exception);

        // Assert
        _loggerMock.Verify(
            a => a.Log(
                LogLevel.Critical,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) =>
                    v.ToString()!.Contains(message)), 
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }
}