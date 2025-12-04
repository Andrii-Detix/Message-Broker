using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.RequeueServices.Exceptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.RequeueServices;

public class RequeueBackgroundService : BackgroundService
{
    private readonly TimeSpan _checkInterval;
    private readonly IRequeueService _requeueService;
    private readonly ILogger<RequeueBackgroundService>? _logger;

    public RequeueBackgroundService(
        IRequeueService? requeueService,
        TimeSpan checkInterval,
        ILogger<RequeueBackgroundService>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(requeueService);

        if (checkInterval <= TimeSpan.Zero)
        {
            throw new CheckIntervalInvalidException();
        }
        
        _requeueService = requeueService;
        _checkInterval = checkInterval;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("Requeue background service started.");

        while (!stoppingToken.IsCancellationRequested)
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
            
            await Task.Delay(_checkInterval, stoppingToken);
        }
        
        _logger?.LogInformation("Requeue Background Service stopped.");
    }
}