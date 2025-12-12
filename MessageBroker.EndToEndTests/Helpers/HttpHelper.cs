using System.Net.Http.Headers;

namespace MessageBroker.EndToEndTests.Helpers;

public static class HttpHelper
{
    public const string MessageIdHeaderName = "X-Message-Id";
    public const string DeliveryAttemptHeaderName = "X-Delivery-Attempts";
    
    public const string PublishUrl = "api/broker/publish";
    public const string ConsumeUrl = "api/broker/consume";
    
    public static string AckUrl(string messageId) => $"api/broker/ack/{messageId}";
    
    public static HttpContent CreateHttpContent(byte[] payload)
    {
        ByteArrayContent content = new(payload);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        return content;
    }
}