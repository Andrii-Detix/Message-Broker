using MessageBroker.EndToEndTests.Extensions;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBroker.EndToEndTests.Abstractions;

public abstract class BaseFunctionalTest : IClassFixture<BrokerFactory>, IDisposable
{
    private WebApplicationFactory<Program> _isolatedFactory;
    private HttpClient _client;
    
    protected readonly string WalDirectory;
    protected HttpClient Client => _client;

    protected BaseFunctionalTest(BrokerFactory factory)
    {
        WalDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(WalDirectory);
        
        var isolatedFactory = factory.WithOptions(new Dictionary<string, string?>
        {
            { "MessageBroker:Wal:Directory", WalDirectory }
        });
        
        _isolatedFactory = isolatedFactory;
        _client = isolatedFactory.CreateClient();
    }

    protected void WithOptions(Dictionary<string, string?> settings)
    {
        var newFactory = _isolatedFactory.WithOptions(settings);
        
        _isolatedFactory.Dispose();
        _client.Dispose();
        
        _isolatedFactory = newFactory;
        _client = newFactory.CreateClient();
    }
    
    protected void ConfigureServices(Action<IServiceCollection> servicesConfiguration)
    {
        var newFactory = _isolatedFactory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureTestServices(servicesConfiguration);
        });

        _isolatedFactory.Dispose();
        _client.Dispose();
        
        _isolatedFactory = newFactory;
        _client = newFactory.CreateClient();
    }
    
    public void Dispose()
    {
        _client.Dispose();

        if (Directory.Exists(WalDirectory))
        {
            try
            {
                Directory.Delete(WalDirectory, true);
            }
            catch
            {
                // ignored
            }
        }
    }
}