using MessageBroker.EndToEndTests.Helpers;
using MessageBroker.LoadTests.Helpers;
using NBomber.Contracts;
using NBomber.CSharp;

namespace MessageBroker.LoadTests.Scenarios;

public static class ConsumeOnlyScenario
{
    public static async Task<ScenarioProps> Create(HttpClient httpClient, LoadTestConfig config, int preseedCount)
    {
        await SeedMessages(preseedCount, config.PayloadMinBytes, config.PayloadMaxBytes, httpClient);
        
        return Scenario.Create(config.ScenarioName, async context =>
            {
                var consumeStep = await BrokerSteps.Consume(context, httpClient);
                
                return consumeStep;
            })
            .WithWarmUpDuration(config.WarmUp)
            .WithLoadSimulations(config.Simulations);
    }

    private static async Task SeedMessages(int count, int minBytes, int maxBytes, HttpClient httpClient)
    {
        var payloads = PayloadGenerator.Generate(count, minBytes, maxBytes);

        List<Task> tasks = [];

        foreach (var payload in payloads)
        {
            HttpContent content = HttpHelper.CreateHttpContent(payload);
            Task task = httpClient.PostAsync(HttpHelper.PublishUrl, content)
                .ContinueWith(_ => content.Dispose());
            tasks.Add(task);
        }
        
        await Task.WhenAll(tasks);
    }
}