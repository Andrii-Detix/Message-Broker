using System.Collections.Concurrent;
using System.Net;
using System.Text;
using DotNet.Testcontainers.Containers;
using MessageBroker.EndToEndTests.Abstractions;
using MessageBroker.EndToEndTests.Extensions;
using MessageBroker.EndToEndTests.Helpers;
using Shouldly;

namespace MessageBroker.EndToEndTests.Durability;

public class DurabilityTests : IDisposable
{
    private readonly string _hostDirectory;

    public DurabilityTests()
    {
        _hostDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_hostDirectory);
    }

    [Fact]
    public async Task Broker_RestoresMessagesAfterCrash_WhenResetOnStartIsDisabled()
    {
        // Arrange
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Broker__ExpiredPolicy__ExpirationTime", "01:00:00" }
        };
        
        int messageCount = 50;
        string[] messages = CreateMessages(messageCount);
        
        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);
        
        await PublishMessages(client1, messages);
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);

        string[] restoredMessages = await ConsumeMessages(client2, messageCount, true);
        
        restoredMessages.Length.ShouldBe(messageCount);
        restoredMessages.ShouldBe(messages, ignoreOrder: false);

        await AssertQueueIsEmpty(client2);

        await broker2.DisposeAsync();
    }
    
    [Fact]
    public async Task Broker_DoesNotRestoreAnyMessage_WhenResetOnStartIsEnabled()
    {
        // Arrange
        int messageCount = 50;
        string[] messages = CreateMessages(messageCount);

        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);

        await PublishMessages(client1, messages);
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, resetOnStart: true);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);
        
        await AssertQueueIsEmpty(client2);
        
        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_RestoresMessagesAfterCrash_WhenMessagesWereRequeued()
    {
        // Arrange
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Broker__ExpiredPolicy__ExpirationTime", "00:00:02" },
            { "MessageBroker__Broker__Requeue__RequeueInterval", "00:00:00.100" },
            { "MessageBroker__Broker__Message__MaxDeliveryAttempts", "100" },
        };
        
        int messageCount = 50;
        string[] messages = CreateMessages(messageCount);

        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);

        await PublishMessages(client1, messages);

        await ConsumeMessages(client1, messageCount, false);
        
        await Task.Delay(TimeSpan.FromSeconds(4));
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);

        string[] restoredMessages = await ConsumeMessages(client2, messageCount, true);
        
        restoredMessages.Length.ShouldBe(messageCount);
        restoredMessages.ShouldBe(messages, ignoreOrder: true);

        await AssertQueueIsEmpty(client2);

        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_DoesNotRestoresDeadMessages_WhenMaxDeliveryAttemptsIsReached()
    {
        // Arrange
        int maxDeliveryAttempts = 2;
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Broker__ExpiredPolicy__ExpirationTime", "00:00:00.010" },
            { "MessageBroker__Broker__Requeue__RequeueInterval", "00:00:00.100" },
            { "MessageBroker__Broker__Message__MaxDeliveryAttempts", $"{maxDeliveryAttempts}" },
        };

        string message = "Message";
        
        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);

        await PublishMessages(client1, message);

        for (int i = 0; i < maxDeliveryAttempts; i++)
        {
            await ConsumeMessages(client1, 1, false);
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);
        
        await AssertQueueIsEmpty(client2);
        
        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_DoesNotRestoreAcknowledgeMessages()
    {
        // Arrange
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Broker__ExpiredPolicy__ExpirationTime", "01:00:00" }
        };
        
        int messageCount = 50;
        string[] messages = CreateMessages(messageCount);

        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);

        await PublishMessages(client1, messages);
        
        await ConsumeMessages(client1, messageCount, ack: true);
        
        await Task.Delay(TimeSpan.FromSeconds(1));
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);
        
        await AssertQueueIsEmpty(client2);
        
        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_RestoresOnlyUnacknowledgedMessages_WhenPartialAcksOccurred()
    {
        // Arrange
        int messageCount = 10;
        string[] messages = CreateMessages(messageCount);
        
        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory);
        await broker1.StartAsync();
        
        HttpClient client1 = CreateClient(broker1);
        
        await PublishMessages(client1, messages);
        
        List<string> unacknowledgedMessage = [];
        
        for (int i = 0; i < messageCount; i++)
        {
            HttpResponseMessage response = await client1.GetAsync(HttpHelper.ConsumeUrl);
            string id = response.ShouldHaveHeader(HttpHelper.MessageIdHeaderName)!;

            if (i % 2 == 0) 
            {
                await client1.PostAsync(HttpHelper.AckUrl(id), null);
            }
            else
            {
                string message = await response.Content.ReadAsStringAsync();
                unacknowledgedMessage.Add(message);
            }
        }
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);

        string[] restoredMessages = await ConsumeMessages(client2, unacknowledgedMessage.Count, true);
        
        restoredMessages.Length.ShouldBe(unacknowledgedMessage.Count);
        restoredMessages.ShouldBe(unacknowledgedMessage, ignoreOrder: true);
        
        await AssertQueueIsEmpty(client2);

        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_RestoresMessagesFromMultipleWalFiles()
    {
        // Arrange
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Wal__MaxWriteCountPerFile", "5" },
            { "MessageBroker__Wal__GarbageCollector__CollectInterval", "01:00:00" },
        };
        
        int messageCount = 50;
        string[] messages = CreateMessages(messageCount);

        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);
        
        await PublishMessages(client1, messages);
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        await broker2.StartAsync();
        
        //Assert
        HttpClient client2 = CreateClient(broker2);

        string[] restoredMessages = await ConsumeMessages(client2, messageCount, true);
        
        restoredMessages.Length.ShouldBe(messageCount);
        restoredMessages.ShouldBe(messages, ignoreOrder: false);

        await AssertQueueIsEmpty(client2);

        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_RestoresCorrectly_WhenGarbageCollectorDeletedOldLogs()
    {
        // Arrange
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Wal__MaxWriteCountPerFile", "5" },
            { "MessageBroker__Wal__GarbageCollector__CollectInterval", "00:00:00.500" },
        };
        
        int ackMessagesCount = 7;
        int messageCount = 50;
        int expectedRestoredMessageCount = messageCount - ackMessagesCount;
        
        string[] messages = CreateMessages(messageCount);
        
        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);

        await PublishMessages(client1, messages);
        
        await ConsumeMessages(client1, ackMessagesCount, ack: true);

        await ConsumeMessages(client1, expectedRestoredMessageCount, ack: false);
        
        await Task.Delay(TimeSpan.FromSeconds(3));
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);
        
        string[] restoredMessages = await ConsumeMessages(client2, expectedRestoredMessageCount, true);
        
        restoredMessages.Length.ShouldBe(expectedRestoredMessageCount);
        restoredMessages.ShouldBe(messages.Skip(ackMessagesCount));
        
        await AssertQueueIsEmpty(client2);

        await broker2.DisposeAsync();
    }

    [Fact]
    public async Task Broker_FailsToStart_WhenWalFileIsCorrupted()
    {
        // Arrange
        Dictionary<string, string> options = new()
        {
            { "MessageBroker__Wal__GarbageCollector__CollectInterval", "01:00:00" },
        };
        
        int messageCount = 50;
        string[] messages = CreateMessages(messageCount);
        
        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        await broker1.StartAsync();

        HttpClient client1 = CreateClient(broker1);
        
        await PublishMessages(client1, messages);
        
        await broker1.StopAsync();
        
        string directoryPath = Path.Combine(_hostDirectory, "wal");
        string enqueueFile = Directory
            .GetFiles(directoryPath, "enqueue-*.log")
            .First(f => !f.Contains("merged"));

        await using (var fs = new FileStream(enqueueFile, FileMode.Open, FileAccess.Write))
        {
            fs.Position = 0;
        
            byte[] garbage = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01, 0x02]; 
            fs.Write(garbage, 0, garbage.Length);
        }
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory, envVars: options);
        
        // Act
        Task actual = broker2.StartAsync();

        // Assert
        await actual.ShouldThrowAsync<Exception>();
    }

    [Fact]
    public async Task Broker_MaintainsDataConsistency_UnderHighConcurrentLoadAndCrash()
    {
        // Arrange
        int producersCount = 5;
        int consumersCount = 5;
        TimeSpan testDuration = TimeSpan.FromSeconds(3);

        ConcurrentBag<string> sentMessages = [];
        ConcurrentBag<string> ackedMessages = [];
        
        IContainer broker1 = BrokerContainerFactory.Create(_hostDirectory);
        await broker1.StartAsync();
        
        using CancellationTokenSource cts = new(testDuration);
        CancellationToken ct = cts.Token;
        
        Task[] producers = Enumerable.Range(0, producersCount)
            .Select(async threadIdx =>
            {
                HttpClient client = CreateClient(broker1);
                int messageIdx = 0;

                while (!ct.IsCancellationRequested)
                {
                    string msgContent = $"Message-{threadIdx}-{messageIdx++}";
                    byte[] payload = Encoding.UTF8.GetBytes(msgContent);
                    using HttpContent content = HttpHelper.CreateHttpContent(payload);

                    try
                    {
                        HttpResponseMessage response = await client.PostAsync(HttpHelper.PublishUrl, content);
                        if (response.StatusCode == HttpStatusCode.Created)
                        {
                            sentMessages.Add(msgContent);
                        }
                    }
                    catch
                    {
                        // Ignore
                    }
                }
            })
            .ToArray();
        
        Task[] consumers = Enumerable.Range(0, consumersCount)
            .Select(async _ =>
            {
                HttpClient client = CreateClient(broker1);

                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        HttpResponseMessage consumeResponse = await client.GetAsync(HttpHelper.ConsumeUrl);
                        if (consumeResponse.StatusCode == HttpStatusCode.OK)
                        {
                            string id = consumeResponse.Headers.GetValues(HttpHelper.MessageIdHeaderName).First();
                            string content = await consumeResponse.Content.ReadAsStringAsync();
                        
                            HttpResponseMessage ackResponse = await client.PostAsync(HttpHelper.AckUrl(id), null);
                            if (ackResponse.StatusCode == HttpStatusCode.OK)
                            {
                                ackedMessages.Add(content);
                            }
                        }
                        else 
                        {
                            await Task.Delay(50); 
                        }
                    }
                    catch
                    {
                        // Ignore
                    }
                }
            })
            .ToArray();
        
        await Task.WhenAll([..producers, ..consumers]);
        
        await broker1.StopAsync();
        
        IContainer broker2 = BrokerContainerFactory.Create(_hostDirectory);
        
        // Act
        await broker2.StartAsync();
        
        // Assert
        HttpClient client2 = CreateClient(broker2);
        
        List<string> restoredMessages = [];
        while (true)
        {
            HttpResponseMessage response = await client2.GetAsync(HttpHelper.ConsumeUrl);
            if (response.StatusCode == HttpStatusCode.NoContent)
            {
                break;
            }

            string content = await response.Content.ReadAsStringAsync();
            restoredMessages.Add(content);
            
            var id = response.Headers.GetValues(HttpHelper.MessageIdHeaderName).First();
            await client2.PostAsync(HttpHelper.AckUrl(id), null);
        }
        
        string[] expectedRestoredMessages = sentMessages.Except(ackedMessages).ToArray();
        int expectedCount = expectedRestoredMessages.Length;
        
        restoredMessages.Count.ShouldBeGreaterThanOrEqualTo(expectedCount);
        expectedRestoredMessages.ShouldBeSubsetOf(restoredMessages);
        ackedMessages.All(m => !restoredMessages.Contains(m)).ShouldBeTrue();
        
        await AssertQueueIsEmpty(client2);
        
        await broker2.DisposeAsync();
    }

    private string[] CreateMessages(int count)
    {
        return Enumerable.Range(0, count)
            .Select(num => $"Message-{num}")
            .ToArray();
    }
    
    private async Task PublishMessages(HttpClient client, params IEnumerable<string> messages)
    {
        foreach (string message in messages)
        {
            byte[] payload = Encoding.UTF8.GetBytes(message);
            using HttpContent content = HttpHelper.CreateHttpContent(payload);
            await client.PostAsync(HttpHelper.PublishUrl, content);
        }
    }
    
    private async Task<string[]> ConsumeMessages(HttpClient client, int expectedCount, bool ack)
    {
        List<string> result = [];
        
        for (int i = 0; i < expectedCount; i++)
        {
            HttpResponseMessage response = await client.GetAsync(HttpHelper.ConsumeUrl);
            if (response.StatusCode == HttpStatusCode.NoContent)
            {
                break;
            }

            result.Add(await response.Content.ReadAsStringAsync());

            if (ack)
            {
                string id = response.ShouldHaveHeader(HttpHelper.MessageIdHeaderName)!; 
                await client.PostAsync(HttpHelper.AckUrl(id), null);
            }
        }
        
        return result.ToArray();
    }
    
    private async Task AssertQueueIsEmpty(HttpClient client)
    {
        HttpResponseMessage response = await client.GetAsync(HttpHelper.ConsumeUrl);
        response.StatusCode.ShouldBe(HttpStatusCode.NoContent);
    }
    
    private HttpClient CreateClient(IContainer container)
    {
        int port = container.GetMappedPublicPort(BrokerContainerFactory.InternalPort);
        return new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{port}")
        };
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_hostDirectory, true);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Cleanup warning: {ex.Message}");
        }
    }
}