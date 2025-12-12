using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.LoadTests.Scenarios;
using NBomber.Contracts;
using NBomber.Contracts.Stats;
using NBomber.CSharp;
using Shouldly;
using Xunit.Abstractions;

namespace MessageBroker.LoadTests.Abstractions;

public abstract class BaseLoadTest(BrokerFactory factory, ITestOutputHelper output)
    : BaseFunctionalTest(factory)
{
    private readonly string _reportDirectory = Path.Combine(AppContext.BaseDirectory, "reports");
    
    protected ITestOutputHelper Output { get; } = output;
    
    protected NodeStats RunNBomber(ScenarioProps scenario, LoadTestConfig config)
    {
        string scenarioDirectory = Path.Combine(_reportDirectory, config.ScenarioName);

        if (!Directory.Exists(scenarioDirectory))
        {
            Directory.CreateDirectory(scenarioDirectory);
        }
        
        NodeStats stats = NBomberRunner
            .RegisterScenarios(scenario)
            .WithReportFileName(config.ScenarioName)
            .WithReportFolder(scenarioDirectory)
            .WithReportFormats(ReportFormat.Txt, ReportFormat.Html)
            .Run();
        
        string mdFilePath = Path.Combine(scenarioDirectory, $"{config.ScenarioName}.txt");

        if (File.Exists(mdFilePath))
        {
            var mdContent = File.ReadAllText(mdFilePath);
            Output.WriteLine(mdContent);
        }
        else 
        {
            Output.WriteLine("Report file not found.");
        }
        
        stats.AllFailCount.ShouldBe(0, "Test failed with errors");
        
        if (stats.ScenarioStats.Length > 0 && stats.ScenarioStats[0].StepStats.Length > 0)
        {
            stats.ScenarioStats[0].StepStats[0].Ok.Request.RPS.ShouldBeGreaterThan(0);
        }

        return stats;
    }
}