using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.LoadTests.Abstractions;
using MessageBroker.LoadTests.Scenarios;
using NBomber.CSharp;
using Xunit.Abstractions;

namespace MessageBroker.LoadTests.Tests;

public class PublishOnlyTests(BrokerFactory factory, ITestOutputHelper output) 
    : BaseLoadTest(factory, output)
{
    [Fact]
    public void PublishOnly_Default_OnePublisher()
    {
        LoadTestConfig config = new()
        {
            ScenarioName = "publish_only_default_one_publisher",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.KeepConstant(copies: 1, TimeSpan.FromSeconds(45))
            ]
        };

        RunNBomber(PublishOnlyScenario.Create(Client, config), config);
    }

    [Fact]
    public void PublishOnly_Default_MultiplePublishers()
    {
        LoadTestConfig config = new()
        {
            ScenarioName = "publish_only_default_multiple_publishers",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.RampingConstant(copies: 10, TimeSpan.FromSeconds(15)),
                Simulation.KeepConstant(copies: 10, TimeSpan.FromSeconds(30))
            ]
        };

        RunNBomber(PublishOnlyScenario.Create(Client, config), config);
    }

    [Fact]
    public void PublishOnly_LargePayloads()
    {
        LoadTestConfig config = new()
        {
            ScenarioName = "publish_only_large_payloads",
            PayloadMinBytes = 1024 * 90,
            PayloadMaxBytes = 1024 * 100,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.RampingConstant(copies: 10, TimeSpan.FromSeconds(15)),
                Simulation.KeepConstant(copies: 10, TimeSpan.FromSeconds(30))
            ]
        };

        RunNBomber(PublishOnlyScenario.Create(Client, config), config);
    }
}