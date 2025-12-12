using MessageBroker.LoadTests.Helpers;
using NBomber.Contracts;
using NBomber.CSharp;
using NBomber.Data.CSharp;

namespace MessageBroker.LoadTests.Scenarios;

public static class FullCycleScenario
{
    public static ScenarioProps Create(HttpClient httpClient, LoadTestConfig config)
    {
        var payloads = PayloadGenerator.Generate(10000, config.PayloadMinBytes, config.PayloadMaxBytes);
        var dataFeed = DataFeed.Circular(payloads);

        return Scenario.Create(config.ScenarioName, async context =>
            {
                byte[] payload = dataFeed.GetNextItem(context.ScenarioInfo);

                var pubStep = await BrokerSteps.Publish(context, httpClient, payload);
                
                if (pubStep.IsError)
                {
                    return pubStep;
                }

                var consumeStep = await BrokerSteps.Consume(context, httpClient);
            
                if (consumeStep.IsError || consumeStep.StatusCode == "204")
                {
                    return consumeStep;
                }

                if (consumeStep.Payload.Value is string messageId)
                {
                    return await BrokerSteps.Ack(context, httpClient, messageId);
                }

                return Response.Fail(message: "Failed to extract Message ID");
            })
            .WithWarmUpDuration(config.WarmUp)
            .WithLoadSimulations(config.Simulations);
    }
}