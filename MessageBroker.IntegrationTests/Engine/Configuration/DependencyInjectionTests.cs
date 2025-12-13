using MessageBroker.Core.Abstractions;
using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.Configurations;
using MessageBroker.Engine.RequeueServices;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Services.GarbageCollector;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

namespace MessageBroker.IntegrationTests.Engine.Configuration;

public class DependencyInjectionTests : IDisposable
{
    private readonly string _directory;

    public DependencyInjectionTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
    }
    
    [Fact]
    public void AddMessageBroker_CreatesAsyncEngine_WhenConfigurationIsValid()
    {
        // Arrange
        IConfiguration configuration = CreateConfiguration();
        IServiceCollection services = CreateServiceCollection(configuration);
        services.AddMessageBroker();
        
        using ServiceProvider provider = services.BuildServiceProvider();

        // Act
        var engine = provider.GetService<IAsyncBrokerEngine>();

        // Assert
        engine.ShouldNotBeNull();
    }

    [Fact]
    public void AddMessageBroker_CreatesRequeueBackgroundService_WhenConfigurationIsValid()
    {
        // Arrange
        IConfiguration configuration = CreateConfiguration();
        IServiceCollection services = CreateServiceCollection(configuration);
        services.AddMessageBroker();
        
        using ServiceProvider provider = services.BuildServiceProvider();

        // Act
        var actual = provider.GetServices<IHostedService>();
        
        // Assert
        var requeueService = actual.OfType<RequeueBackgroundService>().FirstOrDefault();
        requeueService.ShouldNotBeNull();
    }
    
    [Fact]
    public void AddMessageBroker_CreatesGarbageCollectorBackgroundService_WhenConfigurationIsValid()
    {
        // Arrange
        IConfiguration configuration = CreateConfiguration();
        IServiceCollection services = CreateServiceCollection(configuration);
        services.AddMessageBroker();
        
        using ServiceProvider provider = services.BuildServiceProvider();

        // Act
        var actual = provider.GetServices<IHostedService>();
        
        // Assert
        var requeueService = actual.OfType<WalGarbageCollectorBackgroundService>()
            .FirstOrDefault();
        requeueService.ShouldNotBeNull();
    }

    [Fact]
    public void AddMessageBroker_BindsConfigurationToOptionsCorrectly()
    {
        // Arrange
        Dictionary<string, string?> customSettings = new()
        {
            {"MessageBroker:Wal:Directory", "test_wal_dir"},
            {"MessageBroker:Broker:Message:MaxPayloadSize", "5000"}
        };
        
        IConfiguration configuration = CreateConfiguration(customSettings);
        IServiceCollection services = CreateServiceCollection(configuration);
        
        services.AddMessageBroker();
        
        using ServiceProvider provider = services.BuildServiceProvider();

        // Act
        var walOptions = provider.GetRequiredService<WalOptions>();
        var brokerOptions = provider.GetRequiredService<BrokerOptions>();

        // Assert
        walOptions.Directory.ShouldBe("test_wal_dir");
        brokerOptions.Message.MaxPayloadSize.ShouldBe(5000);
    }

    [Fact]
    public void AddMessageBroker_CreatesSingletonBrokerEngine()
    {
        // Arrange
        IConfiguration configuration = CreateConfiguration();
        IServiceCollection services = CreateServiceCollection(configuration);
        services.AddMessageBroker();
        
        ServiceProvider provider = services.BuildServiceProvider();

        // Act
        var queue1 = provider.GetRequiredService<IMessageQueue>();
        var queue2 = provider.GetRequiredService<IMessageQueue>();

        // Assert
        queue1.ShouldBeSameAs(queue2);
    }
    
    private IConfiguration CreateConfiguration(Dictionary<string, string?>? overrides = null)
    {
        Dictionary<string, string?> defaults = new()
        {
            {"MessageBroker:Wal:Directory", _directory}, 
            {"MessageBroker:Wal:MaxWriteCountPerFile", "10"},
            {"MessageBroker:Wal:FileNaming:Extension", "log"},
            {"MessageBroker:Wal:FileNaming:EnqueuePrefix", "enqueue"},
            {"MessageBroker:Wal:FileNaming:AckPrefix", "ack"},
            {"MessageBroker:Wal:FileNaming:DeadPrefix", "dead"},
            {"MessageBroker:Wal:FileNaming:MergePrefix", "merge"},
            {"MessageBroker:Wal:Manifest:FileName", "meta.json"},
            {"MessageBroker:Wal:Manifest:GarbageCollector:CollectInterval", "00:00:05"},
            
            {"MessageBroker:Broker:Message:MaxPayloadSize", "1024"},
            {"MessageBroker:Broker:Message:MaxDeliveryAttempts", "3"},
            {"MessageBroker:Broker:Requeue:RequeueInterval", "00:00:05"},
            {"MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:10"}
        };

        if (overrides != null)
        {
            foreach (var kvp in overrides)
            {
                defaults[kvp.Key] = kvp.Value;
            }
        }

        return new ConfigurationBuilder()
            .AddInMemoryCollection(defaults)
            .Build();
    }

    private IServiceCollection CreateServiceCollection(IConfiguration configuration)
    {
        ServiceCollection services = new();
        
        var mockLifetime = new Mock<IHostApplicationLifetime>();
        services.AddSingleton(mockLifetime.Object);
        
        services.AddSingleton(configuration);

        services.AddLogging();

        return services;
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}