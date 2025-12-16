namespace MessageBroker.EndToEndTests.Abstractions;

public interface IBrokerProcess
{
    Task StartAsync();
    
    Task StopAsync();
    
    HttpClient CreateClient();
}