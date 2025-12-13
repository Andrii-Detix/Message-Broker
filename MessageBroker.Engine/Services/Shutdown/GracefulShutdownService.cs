using MessageBroker.Engine.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.Services.Shutdown;

public class GracefulShutdownService(
    IHostApplicationLifetime appLifetime, 
    ILogger<GracefulShutdownService> logger) 
    : ICriticalErrorService
{
    public void Raise(string message, Exception exception)
    {
        logger.LogCritical(exception, "CRITICAL ERROR: {Message}. Shutting down.", message);
        appLifetime.StopApplication();
    }
}