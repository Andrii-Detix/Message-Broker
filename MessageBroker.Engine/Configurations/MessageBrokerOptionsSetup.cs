using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace MessageBroker.Engine.Configurations;

public class MessageBrokerOptionsSetup(IConfiguration configuration)
    : IConfigureOptions<MessageBrokerOptions>
{
    private const string SectionName = "MessageBroker";
    
    public void Configure(MessageBrokerOptions options)
    {
        configuration.GetSection(SectionName).Bind(options);
    }
}