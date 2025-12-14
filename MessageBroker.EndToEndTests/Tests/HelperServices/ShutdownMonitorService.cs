using Microsoft.Extensions.Hosting;

namespace MessageBroker.EndToEndTests.Tests.HelperServices;

public class ShutdownMonitorService(IHostApplicationLifetime lifetime, TaskCompletionSource<bool> tcs)
    : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        lifetime.ApplicationStopping.Register(() =>
        {
            tcs.TrySetResult(true);
        });

        return Task.CompletedTask;
    }
}