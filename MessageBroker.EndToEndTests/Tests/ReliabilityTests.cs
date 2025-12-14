using System.Net;
using System.Text;
using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.EndToEndTests.Extensions;
using MessageBroker.EndToEndTests.Helpers;
using Shouldly;

namespace MessageBroker.EndToEndTests.Tests;

public class ReliabilityTests(BrokerFactory factory) : BaseFunctionalTest(factory)
{
    [Fact]
    public async Task Ack_ReturnsNotFound_WhenMessageIsAlreadyAcked()
    {
        // Arrange
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:01:00" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:01:00"}
        });
        
        using HttpContent content = HttpHelper.CreateHttpContent([]);
        
        await Client.PostAsync(HttpHelper.PublishUrl, content);
        
        HttpResponseMessage consumeResponse = await Client.GetAsync(HttpHelper.ConsumeUrl);
        string messageId = consumeResponse.ShouldHaveHeader(HttpHelper.MessageIdHeaderName)!;
        
        string ackUrl = HttpHelper.AckUrl(messageId);
        
        await Client.PostAsync(ackUrl, null);
        
        // Act
        HttpResponseMessage actual = await Client.PostAsync(ackUrl, null);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }
    
    [Fact]
    public async Task Ack_ReturnsNotFound_WhenConsumedMessageIsRequeued()
    {
        // Arrange
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:00:00.050" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:00.010"}
        });
        
        using HttpContent content = HttpHelper.CreateHttpContent([]);
        
        await Client.PostAsync(HttpHelper.PublishUrl, content);
        
        HttpResponseMessage consumeResponse = await Client.GetAsync(HttpHelper.ConsumeUrl);
        string messageId = consumeResponse.ShouldHaveHeader(HttpHelper.MessageIdHeaderName)!;
        
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        
        // Act
        HttpResponseMessage actual = await Client.PostAsync(HttpHelper.AckUrl(messageId), null);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }
    
    [Fact]
    public async Task Broker_RequeuesMessage_WhenExpiredTimeForAcknowledgementIsReached()
    {
        // Arrange
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:00:00.050" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:00.010"}
        });
        
        string payloadText = "Some payload representation";
        byte[] payload = Encoding.UTF8.GetBytes(payloadText);
        
        using HttpContent content = HttpHelper.CreateHttpContent(payload);
        
        await Client.PostAsync(HttpHelper.PublishUrl, content);
        
        HttpResponseMessage response1 = await Client.GetAsync(HttpHelper.ConsumeUrl);
        
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        
        // Act
        HttpResponseMessage response2 = await Client.GetAsync(HttpHelper.ConsumeUrl);
        
        // Assert
        response2.StatusCode.ShouldBe(HttpStatusCode.OK);
        string actualText = await response2.Content.ReadAsStringAsync();
        
        actualText.ShouldNotBeNullOrEmpty();
        actualText.ShouldBe(payloadText);

        string id = response2.ShouldHaveHeader(HttpHelper.MessageIdHeaderName)!;
        id.ShouldBe(response1.ShouldHaveHeader(HttpHelper.MessageIdHeaderName));
        
        response2.ShouldHaveHeader(HttpHelper.DeliveryAttemptHeaderName, 2.ToString());
    }

    [Fact]
    public async Task Broker_MoveMessageToTheEndOfQueue_WhenMessageIsRequeued()
    {
        // Arrange
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:00:00.050" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:00.010"}
        });

        string[] messages = ["Message-1", "Message-2", "Message-3"];
        string[] expectedConsumedOrder = ["Message-2", "Message-3", "Message-1"];

        foreach (string message in messages)
        {
            byte[] payload = Encoding.UTF8.GetBytes(message);
            using HttpContent content = HttpHelper.CreateHttpContent(payload);
            await Client.PostAsync(HttpHelper.PublishUrl, content);
        }
        
        await Client.GetAsync(HttpHelper.ConsumeUrl);
        
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        
        List<HttpResponseMessage> responses = [];
        
        // Act
        for (int i = 0; i < 3; i++)
        {
            HttpResponseMessage actual = await Client.GetAsync(HttpHelper.ConsumeUrl);
            responses.Add(actual);
        }
        
        // Assert
        List<string> actualConsumedOrder = [];

        foreach (var response in responses)
        {
            response.StatusCode.ShouldBe(HttpStatusCode.OK);
            
            string actualText = await response.Content.ReadAsStringAsync();
            actualConsumedOrder.Add(actualText);
        }
        
        actualConsumedOrder.ShouldBe(expectedConsumedOrder);
    }
    
    [Fact]
    public async Task Broker_MovesMessageToDeadState_WhenMaxDeliveryAttemptsIsReached()
    {
        // Arrange
        WithOptions(new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "00:00:00.050" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "00:00:00.010" },
            { "MessageBroker:Broker:Message:MaxDeliveryAttempts", "3" }
        });
        
        string payloadText = "Some payload representation";
        byte[] payload = Encoding.UTF8.GetBytes(payloadText);
        
        using HttpContent content = HttpHelper.CreateHttpContent(payload);
        
        await Client.PostAsync(HttpHelper.PublishUrl, content);

        List<HttpResponseMessage> responses = [];
        
        // Act
        for (int i = 0; i < 4; i++)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            
            HttpResponseMessage response = await Client.GetAsync(HttpHelper.ConsumeUrl);
            responses.Add(response);
        }
        
        // Assert
        for (int i = 0; i < 3; i++)
        {
            HttpResponseMessage actual = responses[i];
            
            actual.StatusCode.ShouldBe(HttpStatusCode.OK);
            actual.ShouldHaveHeader(HttpHelper.DeliveryAttemptHeaderName, (i + 1).ToString());
        }
        
        responses[3].StatusCode.ShouldBe(HttpStatusCode.NoContent);
    }
}