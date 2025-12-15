using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.LoadTests.Abstractions;
using MessageBroker.LoadTests.Scenarios;
using NBomber.Contracts;
using NBomber.CSharp;
using Xunit.Abstractions;

namespace MessageBroker.LoadTests.Tests;

public class ConsumeOnlyTests(BrokerFactory factory, ITestOutputHelper output) 
    : BaseLoadTest(factory, output)
{
    [Fact]
    public async Task ConsumeOnly_Default_OneConsumer()
    {
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:00:00.500" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:00.010"},
            { "MessageBroker:Broker:Message:MaxDeliveryAttempts", int.MaxValue.ToString() }
        });
        
        LoadTestConfig config = new()
        {
            ScenarioName = "consume_only_default_one_consumer",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.KeepConstant(copies: 1, TimeSpan.FromSeconds(45))
            ]
        };
        
        ScenarioProps scenario = await ConsumeOnlyScenario.Create(Client, config, 100000);
        RunNBomber(scenario, config);
    }
    
    [Fact]
    public async Task ConsumeOnly_Default_MultipleConsumers()
    {
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:00:00.500" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:00.010"},
            { "MessageBroker:Broker:Message:MaxDeliveryAttempts", int.MaxValue.ToString() }
        });
        
        LoadTestConfig config = new()
        {
            ScenarioName = "consume_only_default_multiple_consumers",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.RampingConstant(copies: 10, TimeSpan.FromSeconds(15)),
                Simulation.KeepConstant(copies: 10, TimeSpan.FromSeconds(30))
            ]
        };
        
        ScenarioProps scenario = await ConsumeOnlyScenario.Create(Client, config, 100000);
        RunNBomber(scenario, config);
    }
}