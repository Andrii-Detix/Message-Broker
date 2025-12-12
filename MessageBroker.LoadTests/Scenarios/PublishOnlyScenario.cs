using MessageBroker.LoadTests.Helpers;
using NBomber.Contracts;
using NBomber.CSharp;
using NBomber.Data;
using NBomber.Data.CSharp;

namespace MessageBroker.LoadTests.Scenarios;

public static class PublishOnlyScenario
{
    public static ScenarioProps Create(HttpClient httpClient, LoadTestConfig config)
    {
        byte[][] payloads = PayloadGenerator.Generate(10000, config.PayloadMinBytes, config.PayloadMaxBytes);

        IDataFeed<byte[]> dataFeed = DataFeed.Circular(payloads);
        
        return Scenario.Create(config.ScenarioName, async context =>
            {
                byte[] payload = dataFeed.GetNextItem(context.ScenarioInfo);

                var step = await BrokerSteps.Publish(context, httpClient, payload);

                return step;
            })
            .WithWarmUpDuration(config.WarmUp)
            .WithLoadSimulations(config.Simulations);
    }
}