using NBomber.Contracts;
using NBomber.CSharp;

namespace MessageBroker.LoadTests.Scenarios;

public record LoadTestConfig
{
    public string ScenarioName { get; init; } = "default_load";
    
    public int PayloadMinBytes { get; init; } = 512;
    public int PayloadMaxBytes { get; init; } = 4096;
    
    public TimeSpan WarmUp { get; init; } = TimeSpan.FromSeconds(5);

    public LoadSimulation[] Simulations { get; init; } = 
    [
        Simulation.RampingConstant(copies: 100, during: TimeSpan.FromSeconds(10)),
        Simulation.KeepConstant(copies: 100, during: TimeSpan.FromSeconds(30))
    ];
}