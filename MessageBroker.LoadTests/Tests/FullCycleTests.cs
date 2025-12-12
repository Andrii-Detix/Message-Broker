using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.LoadTests.Abstractions;
using MessageBroker.LoadTests.Scenarios;
using NBomber.CSharp;
using Xunit.Abstractions;

namespace MessageBroker.LoadTests.Tests;

public class FullCycleTests(BrokerFactory factory, ITestOutputHelper output) 
    : BaseLoadTest(factory, output)
{
    [Fact]
    public void FullCycle_Default_OneClient()
    {
        LoadTestConfig config = new()
        {
            ScenarioName = "full_cycle_default_one_client",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.KeepConstant(copies: 1, TimeSpan.FromSeconds(45))
            ]
        };
        
        RunNBomber(FullCycleScenario.Create(Client, config), config);
    }

    [Fact]
    public void FullCycle_Default_MultipleClients()
    {
        LoadTestConfig config = new()
        {
            ScenarioName = "full_cycle_default_multiple_clients",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.RampingConstant(copies: 10, TimeSpan.FromSeconds(15)),
                Simulation.KeepConstant(copies: 10, TimeSpan.FromSeconds(30))
            ]
        };
        
        RunNBomber(FullCycleScenario.Create(Client, config), config);
    }

    [Fact]
    public void FullCycle_LargePayloads()
    {
        LoadTestConfig config = new()
        {
            ScenarioName = "full_cycle_large_payloads",
            PayloadMinBytes = 1024 * 90,
            PayloadMaxBytes = 1024 * 100,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.RampingConstant(copies: 10, TimeSpan.FromSeconds(15)),
                Simulation.KeepConstant(copies: 10, TimeSpan.FromSeconds(30))
            ]
        };
        
        RunNBomber(FullCycleScenario.Create(Client, config), config);
    }

    [Fact]
    public void FullCycle_SoakTest_WithGarbageCollection()
    {
        WithOptions(new()
        {
            { "MessageBroker:Wal:GarbageCollector:CollectInterval", "00:01:00" }
        });
        
        LoadTestConfig config = new()
        {
            ScenarioName = "full_cycle_soak_test_with_garbage_collection",
            PayloadMinBytes = 1024,
            PayloadMaxBytes = 4096,
            WarmUp = TimeSpan.FromSeconds(5),
            Simulations = 
            [
                Simulation.RampingConstant(copies: 10, TimeSpan.FromSeconds(15)),
                Simulation.KeepConstant(copies: 10, TimeSpan.FromMinutes(5))
            ]
        };
        
        RunNBomber(FullCycleScenario.Create(Client, config), config);
    }
}