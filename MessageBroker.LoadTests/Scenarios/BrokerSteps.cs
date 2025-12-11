using System.Net;
using MessageBroker.EndToEndTests.Helpers;
using NBomber.Contracts;
using NBomber.CSharp;

namespace MessageBroker.LoadTests.Scenarios;

public static class BrokerSteps
{
    public static Task<Response<object>> Publish(IScenarioContext context, HttpClient httpClient, byte[] payload)
    {
        return Step.Run("publish", context, async () =>
        {
            using var content = HttpHelper.CreateHttpContent(payload);
            var response = await httpClient.PostAsync(HttpHelper.PublishUrl, content);

            var code = ((int)response.StatusCode).ToString();

            return response.IsSuccessStatusCode
                ? Response.Ok(sizeBytes: payload.Length, statusCode: code)
                : Response.Fail(statusCode: code, message: response.ReasonPhrase);
        });
    }

    public static Task<Response<object>> Consume(IScenarioContext context, HttpClient httpClient)
    {
        return Step.Run("consume", context, async () =>
        {
            var response = await httpClient.GetAsync(HttpHelper.ConsumeUrl);
            var code = ((int)response.StatusCode).ToString();

            if (response.StatusCode == HttpStatusCode.OK)
            {
                if (response.Headers.TryGetValues(HttpHelper.MessageIdHeaderName, out var values))
                {
                    string messageId = values.First();
                    return Response.Ok<object>(payload: messageId, statusCode: code);
                }
                
                return Response.Fail(statusCode: code, message: "Missing X-Message-Id header");
            }
            
            if (response.StatusCode == HttpStatusCode.NoContent)
            {
                return Response.Ok(statusCode: code);
            }

            return Response.Fail(statusCode: code, message: "Unexpected status code");
        });
    }

    public static Task<Response<object>> Ack(IScenarioContext context, HttpClient httpClient, string messageId)
    {
        return Step.Run("ack", context, async () =>
        {
            var response = await httpClient.PostAsync(HttpHelper.AckUrl(messageId), null);
            var code = ((int)response.StatusCode).ToString();
            
            return response.IsSuccessStatusCode
                ? Response.Ok(statusCode: code)
                : Response.Fail(statusCode: code, message: response.ReasonPhrase);
        });
    }
}