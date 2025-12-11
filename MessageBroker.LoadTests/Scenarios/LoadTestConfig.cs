namespace MessageBroker.LoadTests.Scenarios;

public record LoadTestConfig
{
    public string ScenarioName { get; init; } = "default_load";
    
    public int PayloadMinBytes { get; init; } = 512;
    public int PayloadMaxBytes { get; init; } = 4096;
    
    public int VirtualUsers { get; init; } = 100;
    
    public TimeSpan WarmUp { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan RampUp { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan KeepStable { get; init; } = TimeSpan.FromSeconds(30);
}