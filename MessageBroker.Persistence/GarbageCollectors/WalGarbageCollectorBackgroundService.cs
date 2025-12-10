using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.GarbageCollectors.Exceptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Persistence.GarbageCollectors;

public class WalGarbageCollectorBackgroundService : BackgroundService
{
    private readonly GarbageCollectorOptions _options;
    private readonly IWalGarbageCollectorService _gcService;
    private readonly ILogger<WalGarbageCollectorBackgroundService> _logger;

    public WalGarbageCollectorBackgroundService(
        IWalGarbageCollectorService gcService,
        GarbageCollectorOptions options,
        ILogger<WalGarbageCollectorBackgroundService> logger)
    {
        ArgumentNullException.ThrowIfNull(gcService);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        if (options.CollectInterval <= TimeSpan.Zero)
        {
            throw new CollectIntervalInvalidException();
        }
        
        _gcService = gcService;
        _options = options;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Wal garbage collector service started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _gcService.Collect();
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex, 
                    "Error occurred during collecting with message: '{ErrorMessage}'.", 
                    ex.Message);
            }
            
            await Task.Delay(_options.CollectInterval, stoppingToken);
        }
        
        _logger.LogInformation("Wal garbage collector service stopped.");
    }
}