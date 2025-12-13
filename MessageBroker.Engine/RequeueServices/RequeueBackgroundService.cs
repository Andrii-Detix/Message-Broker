using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.RequeueServices.Exceptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.RequeueServices;

public class RequeueBackgroundService : BackgroundService
{
    private readonly TimeSpan _checkInterval;
    private readonly IRequeueService _requeueService;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RequeueBackgroundService>? _logger;

    public RequeueBackgroundService(
        IRequeueService? requeueService,
        TimeSpan checkInterval, 
        TimeProvider timeProvider,
        ILogger<RequeueBackgroundService>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(requeueService);
        ArgumentNullException.ThrowIfNull(timeProvider);

        if (checkInterval <= TimeSpan.Zero)
        {
            throw new CheckIntervalInvalidException();
        }
        
        _requeueService = requeueService;
        _checkInterval = checkInterval;
        _timeProvider = timeProvider;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("Requeue background service started.");
        
        using PeriodicTimer timer = new PeriodicTimer(_checkInterval, _timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    _requeueService.Requeue();
                }
                catch (Exception ex)
                {
                    _logger?.LogError(
                        ex,
                        "Error occurred during requeuing with message '{ErrorMessage}'.",
                        ex.Message);
                }
            }
        }
        finally
        {
            _logger?.LogInformation("Requeue background service stopped.");
        }
    }
}