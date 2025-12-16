using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.EndToEndTests.Extensions;
using Microsoft.AspNetCore.Mvc.Testing;

namespace MessageBroker.EndToEndTests.BrokerProcesses;

public class InMemoryBrokerProcess(
    string? hostDirectory = null,
    bool resetOnStart = false,
    Dictionary<string, string?>? envVars = null) 
    : IBrokerProcess
{
    private readonly WebApplicationFactory<Program> _factory = Create(hostDirectory, resetOnStart, envVars);
    
    public Task StartAsync()
    {
        _factory.StartServer();
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        await _factory.DisposeAsync();
    }

    public HttpClient CreateClient()
    {
        return _factory.CreateClient();
    }

    private static WebApplicationFactory<Program> Create(
        string? hostDirectory = null,
        bool resetOnStart = false,
        Dictionary<string, string?>? envVars = null)
    {
        WebApplicationFactory<Program> factory = new WebApplicationFactory<Program>()
            .WithOption("MessageBroker:Wal:ResetOnStartup", resetOnStart.ToString());
        
        if (!string.IsNullOrWhiteSpace(hostDirectory))
        {
            factory = factory.WithOption("MessageBroker:Wal:Directory", hostDirectory);
        }

        if (envVars is not null)
        {
            factory = factory.WithOptions(envVars);
        }

        return factory;
    }
}