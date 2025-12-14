using System.Net;
using System.Text;
using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.EndToEndTests.Helpers;
using Shouldly;

namespace MessageBroker.EndToEndTests.Tests;

public class ComplexTests(BrokerFactory factory) : BaseFunctionalTest(factory)
{
    [Fact]
    public async Task Publish_DoesNotLostAnyMessage_WhenPublishesConcurrently()
    {
        // Arrange
        Dictionary<string, string?> options = new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "01:00:00" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "01:00:00"}
        };
        WithOptions(options);
        
        int threadCount = 5;
        int messagesPerThread = 1000;
        int totalMessageCount = threadCount * messagesPerThread;
        
        string[] textPayloads = Enumerable.Range(0, totalMessageCount)
            .Select(num => $"Message-{num}")
            .ToArray();
        byte[][] payloads = textPayloads.Select(Encoding.UTF8.GetBytes).ToArray();
        
        // Act
        Task[] tasks = Enumerable.Range(0, threadCount)
            .Select(threadIdx => Task.Run(async () =>
            {
                int from = threadIdx *  messagesPerThread;
                int to = (threadIdx + 1) * messagesPerThread;

                for (; from < to; from++)
                {
                    using HttpContent content = HttpHelper.CreateHttpContent(payloads[from]);
                    await Client.PostAsync(HttpHelper.PublishUrl, content);
                }
            }))
            .ToArray();
        
        await Task.WhenAll(tasks);
        
        // Assert
        List<string> actualTextPayloads = [];

        for (int i = 0; i < totalMessageCount; i++)
        {
            HttpResponseMessage response = await Client.GetAsync(HttpHelper.ConsumeUrl);
            if (response.StatusCode == HttpStatusCode.OK)
            {
                string actualTextPayload = await response.Content.ReadAsStringAsync();
                actualTextPayloads.Add(actualTextPayload);
            }
        }
        
        actualTextPayloads.Count.ShouldBe(totalMessageCount);
        actualTextPayloads.ShouldBe(textPayloads, ignoreOrder: true);
        
        HttpResponseMessage noContentResponse = await Client.GetAsync(HttpHelper.ConsumeUrl);
        noContentResponse.StatusCode.ShouldBe(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task Consume_ReturnsUniqueMessages_WhenConsumesConcurrently()
    {
        // Arrange
        Dictionary<string, string?> options = new()
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "01:00:00" },
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "01:00:00"}
        };
        WithOptions(options);
        
        int threadCount = 5;
        int messagesPerThread = 1000;
        int totalMessageCount = threadCount * messagesPerThread;
        
        var payloadIndexMap = Enumerable.Range(0, totalMessageCount)
            .ToDictionary(i => $"Message-{i}", i => i);
        
        string[] textPayloads = payloadIndexMap.Keys.ToArray();
        byte[][] payloads = textPayloads.Select(Encoding.UTF8.GetBytes).ToArray();

        foreach (byte[] payload in payloads)
        {
            using HttpContent content = HttpHelper.CreateHttpContent(payload);
            await Client.PostAsync(HttpHelper.PublishUrl, content);
        }
        
        // Act
        Task<List<string>>[] tasks = Enumerable.Range(0, threadCount)
            .Select(_ => Task.Run(async () =>
            {
                List<string> localReceived = [];
                
                for (int i = 0; i < messagesPerThread; i++)
                {
                    HttpResponseMessage response = await Client.GetAsync(HttpHelper.ConsumeUrl);

                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        string text = await response.Content.ReadAsStringAsync();
                        localReceived.Add(text);
                    }
                }
                
                return localReceived;
            }))
            .ToArray();
        
        List<string>[] actualTextPayloads = await Task.WhenAll(tasks);
        
        // Assert
        actualTextPayloads.Sum(threadPayloads => threadPayloads.Count).ShouldBe(totalMessageCount);

        foreach (var threadPayloads in actualTextPayloads)
        {
            for (int i = 0; i < messagesPerThread - 1; i++)
            {
                int curPayloadIdx = payloadIndexMap[threadPayloads[i]];
                int nextPayloadIdx = payloadIndexMap[threadPayloads[i+1]];
                
                curPayloadIdx.ShouldBeLessThan(nextPayloadIdx);
            }
        }
        
        string[] allActualTextPayloads = actualTextPayloads
            .SelectMany(tp => tp)
            .ToArray();
        allActualTextPayloads.ShouldBe(textPayloads, ignoreOrder: true);
        
        HttpResponseMessage noContentResponse = await Client.GetAsync(HttpHelper.ConsumeUrl);
        noContentResponse.StatusCode.ShouldBe(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task System_ProcessesMessages_WhenPublishingAndConsumingSimultaneously()
    {
        // Arrange
        WithOptions(new Dictionary<string, string?>
        {
            { "MessageBroker:Broker:Requeue:RequeueInterval", "01:00:00" }, 
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "01:00:00"}
        });
        
        int messageCount = 5000;

        List<string> publishedData = [];
        List<string> consumedData = [];
        
        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(15));
        CancellationToken ct = cts.Token;
        
        // Act
        Task producerTask = Task.Run(async () =>
        {
            for (int i = 0; i < messageCount; i++)
            {
                string textPayload = $"Message-{i}";
                byte[] payload = Encoding.UTF8.GetBytes(textPayload);
                using HttpContent content = HttpHelper.CreateHttpContent(payload);
                
                await Client.PostAsync(HttpHelper.PublishUrl, content, ct);
                publishedData.Add(textPayload);
            }
        }, ct);
        
        Task consumerTask = Task.Run(async () =>
        {
            while (consumedData.Count < messageCount)
            {
                HttpResponseMessage result = await Client.GetAsync(HttpHelper.ConsumeUrl, ct);
                
                if (result.StatusCode == HttpStatusCode.OK)
                {
                    string payload = await result.Content.ReadAsStringAsync(ct);
                    consumedData.Add(payload);
                }
                else
                {
                    await Task.Delay(10, ct);
                }
            }
        }, ct);
        
        await Task.WhenAll(producerTask, consumerTask);
        
        // Assert
        consumedData.Count.ShouldBe(messageCount);
        consumedData.ShouldBe(publishedData);
    }

    [Fact]
    public async Task GarbageCollector_DeletesOldLogFiles_WhenMessagesAreAcked()
    {
        // Arrange
        WithOptions(new Dictionary<string, string?>
        {
            { "MessageBroker:Wal:MaxWriteCountPerFile", "2" },
            { "MessageBroker:Wal:GarbageCollector:CollectInterval", "00:00:00.500" },
            { "MessageBroker:Broker:Requeue:RequeueInterval", "01:00:00" }, 
            { "MessageBroker:Broker:ExpiredPolicy:ExpirationTime", "01:00:00"}
        });
        
        int messageCount = 16;
        List<string> messageIds = [];
        
        for (int i = 0; i < messageCount; i++)
        {
            byte[] payload = Encoding.UTF8.GetBytes($"Message-{i}");
            using HttpContent content = HttpHelper.CreateHttpContent(payload);
            await Client.PostAsync(HttpHelper.PublishUrl, content);
            
            HttpResponseMessage result = await Client.GetAsync(HttpHelper.ConsumeUrl);
            
            string id = result.Headers.GetValues(HttpHelper.MessageIdHeaderName).First();
            messageIds.Add(id);
        }
        
        string[] logFilesBefore = Directory.GetFiles(WalDirectory, "enqueue-*.log")
            .Where(f => !Path.GetFileName(f).Contains("merged")) 
            .ToArray();
        
        // Act
        foreach (string id in messageIds)
        {
            await Client.PostAsync(HttpHelper.AckUrl(id), null);
        }

        await Task.Delay(1500);
        
        // Assert
        string[] logFilesAfter = Directory.GetFiles(WalDirectory, "enqueue-*.log")
            .Where(f => !Path.GetFileName(f).Contains("merged"))
            .ToArray();
        
        string[] mergedEnqueueFiles = Directory.GetFiles(WalDirectory, "enqueue-merged-*.log")
            .ToArray();
        
        logFilesBefore.Length.ShouldBeGreaterThanOrEqualTo(8);
        logFilesAfter.Length.ShouldBeLessThanOrEqualTo(logFilesBefore.Length);
        
        mergedEnqueueFiles.Length.ShouldBe(1);
    }
}