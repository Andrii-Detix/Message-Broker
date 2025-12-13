using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Services.GarbageCollector.Exceptions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Persistence.Services.GarbageCollector;

public class WalGarbageCollectorBackgroundService : BackgroundService
{
    private readonly GarbageCollectorOptions _options;
    private readonly IWalGarbageCollectorService _gcService;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<WalGarbageCollectorBackgroundService> _logger;

    public WalGarbageCollectorBackgroundService(
        IWalGarbageCollectorService gcService,
        GarbageCollectorOptions options,
        TimeProvider timeProvider,
        ILogger<WalGarbageCollectorBackgroundService> logger)
    {
        ArgumentNullException.ThrowIfNull(gcService);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        if (options.CollectInterval <= TimeSpan.Zero)
        {
            throw new CollectIntervalInvalidException();
        }
        
        _gcService = gcService;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Wal garbage collector service started.");
        
        using PeriodicTimer timer = new PeriodicTimer(_options.CollectInterval, _timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
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
            }
        }
        finally
        {
            _logger.LogInformation("Wal garbage collector service stopped.");
        }
    }
}