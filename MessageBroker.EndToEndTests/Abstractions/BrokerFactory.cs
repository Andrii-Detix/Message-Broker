using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace MessageBroker.EndToEndTests.Abstractions;

public class BrokerFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        
        string testConfigPath = Path.Combine(AppContext.BaseDirectory, "appsettings.Testing.json");
        
        builder.ConfigureAppConfiguration((_, config) =>
        {
            config.AddJsonFile(testConfigPath, optional: false, reloadOnChange: true);
        });
    }
}