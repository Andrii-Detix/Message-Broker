using System.Net;
using System.Text;
using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.EndToEndTests.Extensions;
using Shouldly;

namespace MessageBroker.EndToEndTests.Tests;

public class SmokeTests(BrokerFactory factory) : BaseFunctionalTest(factory)
{
    [Fact]
    public async Task Publish_ReturnsOk_WhenPayloadIsValid()
    {
        // Arrange
        string payloadText = "Some payload representation";
        byte[] payload = Encoding.UTF8.GetBytes(payloadText);
        
        using HttpContent content = CreateHttpContent(payload);
        
        // Act
        HttpResponseMessage actual = await Client.PostAsync(PublishUrl, content);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.Created);
    }

    [Fact]
    public async Task Publish_ReturnsTooLarge_WhenPayloadIsTooLarge()
    {
        // Arrange
        Dictionary<string, string?> options = new()
        {
            { "MessageBroker:Broker:Message:MaxPayloadSize", "2"}
        };
        WithOptions(options);

        byte[] payload = [0x01, 0x02, 0x03];
        
        using HttpContent content = CreateHttpContent(payload);
        
        // Act
        HttpResponseMessage actual = await Client.PostAsync(PublishUrl, content);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Consume_ReturnsNoContent_WhenNoMessagesExist()
    {
        // Act
        HttpResponseMessage actual = await Client.GetAsync(ConsumeUrl);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Consume_ReturnsMessage_WhenMessageExists()
    {
        // Arrange
        string payloadText = "Some payload representation";
        byte[] payload = Encoding.UTF8.GetBytes(payloadText);
        
        using HttpContent content = CreateHttpContent(payload);
        
        await Client.PostAsync(PublishUrl, content);
        
        // Act
        HttpResponseMessage actual = await Client.GetAsync(ConsumeUrl);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.OK);
        string actualText = await actual.Content.ReadAsStringAsync();
        
        actualText.ShouldNotBeNullOrEmpty();
        actualText.ShouldBe(payloadText);
    }
    
    [Fact]
    public async Task Consume_ReturnsHeaders_WhenMessageExists()
    {
        // Arrange
        using HttpContent content = CreateHttpContent([]);
        
        await Client.PostAsync(PublishUrl, content);
        
        // Act
        HttpResponseMessage actual = await Client.GetAsync(ConsumeUrl);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.OK);

        string? messageId = actual.ShouldHaveHeader("X-Message-Id");
        messageId.ShouldNotBeNullOrEmpty();
        
        actual.ShouldHaveHeader("X-Delivery-Attempts", 1.ToString());
    }

    [Fact]
    public async Task Consume_ReturnsOnlyOneMessage_WhenOnlyOneMessageIsPublished()
    {
        // Arrange
        using HttpContent content = CreateHttpContent([]);
        
        await Client.PostAsync(PublishUrl, content);
        await Client.GetAsync(ConsumeUrl);
        
        // Act
        HttpResponseMessage actual = await Client.GetAsync(ConsumeUrl);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Consume_ReturnsMessagesInFifoOrder_WhenSeveralMessagesArePublished()
    {
        // Arrange
        string[] messages = ["msg1", "msg2", "msg3"];

        foreach (string message in messages)
        {
            byte[] payload = Encoding.UTF8.GetBytes(message);
            using HttpContent content = CreateHttpContent(payload);
            await Client.PostAsync(PublishUrl, content);
        }

        List<HttpResponseMessage> responses = [];

        // Act
        for (int i = 0; i < messages.Length; i++)
        {
            HttpResponseMessage response = await Client.GetAsync(ConsumeUrl);
            responses.Add(response);
        }
        
        // Assert
        for (int i = 0; i < messages.Length; i++)
        {
            HttpResponseMessage actual = responses[i];
            
            actual.StatusCode.ShouldBe(HttpStatusCode.OK);
            string actualText = await actual.Content.ReadAsStringAsync();
        
            actualText.ShouldNotBeNullOrEmpty();
            actualText.ShouldBe(messages[i]);
        }
    }

    [Fact]
    public async Task Ack_ReturnsOk_WhenMessageIsInFlight()
    {
        // Arrange
        using HttpContent content = CreateHttpContent([]);
        
        await Client.PostAsync(PublishUrl, content);
        
        HttpResponseMessage consumeResponse = await Client.GetAsync(ConsumeUrl);
        string messageId = consumeResponse.ShouldHaveHeader("X-Message-Id")!;
        
        // Act
        HttpResponseMessage actual = await Client.PostAsync($"{AckUrl}/{messageId}", null);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.OK);
    }
    
    [Fact]
    public async Task Ack_ReturnsNotFound_WhenMessageWithIdIsNotInFlight()
    {
        // Arrange
        using HttpContent content = CreateHttpContent([]);
        
        await Client.PostAsync(PublishUrl, content);
        
        HttpResponseMessage consumeResponse = await Client.GetAsync(ConsumeUrl);
        string messageId = consumeResponse.ShouldHaveHeader("X-Message-Id")!;

        string url = $"{AckUrl}/{messageId}";
        
        await Client.PostAsync(url, null);

        // Act
        HttpResponseMessage actual = await Client.PostAsync(url, null);
        
        // Assert
        actual.StatusCode.ShouldBe(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Broker_HandlesArbitraryBinaryData()
    {
        // Arrange
        byte[] payload = [0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00];
        
        using HttpContent content = CreateHttpContent(payload);
    
        await Client.PostAsync(PublishUrl, content);
        
        // Act
        HttpResponseMessage actual = await Client.GetAsync(ConsumeUrl);
        
        // Assert
        byte[] actualPayload = await actual.Content.ReadAsByteArrayAsync();
        actualPayload.ShouldBe(payload);
    }
}