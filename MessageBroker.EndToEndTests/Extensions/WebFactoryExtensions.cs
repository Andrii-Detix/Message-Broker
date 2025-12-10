using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace MessageBroker.EndToEndTests.Extensions;

public static class WebFactoryExtensions
{
    extension<T>(WebApplicationFactory<T> factory) 
        where T : class
    {
        public WebApplicationFactory<T> WithOptions(Dictionary<string, string?> settings)
        {
            return factory.WithWebHostBuilder(builder =>
            {
                builder.ConfigureAppConfiguration((_, config) =>
                {
                    config.AddInMemoryCollection(settings);
                });
            });
        }
        
        public WebApplicationFactory<T> WithOption(string key, string value)
        {
            return factory.WithOptions(new Dictionary<string, string?> 
            { 
                { key, value } 
            });
        }
    }
}