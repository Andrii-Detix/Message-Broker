using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.Manifests;
using MessageBroker.Persistence.Recovery;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.Recovery;

public class RecoveryServiceTests
{
    private readonly Mock<IManifestManager> _manifestMock;
    private readonly Mock<IWalReader<EnqueueWalEvent>> _enqueueReaderMock;
    private readonly Mock<IWalReader<AckWalEvent>> _ackReaderMock;
    private readonly Mock<IWalReader<DeadWalEvent>> _deadReaderMock;
    private readonly Mock<IMessageQueueFactory> _queueFactoryMock;
    private readonly FakeTimeProvider _timeProvider;
    private readonly WalOptions _walOptions;
    private readonly MessageOptions _messageOptions;
    
    public RecoveryServiceTests()
    {
        _manifestMock = new();
        _enqueueReaderMock = new();
        _ackReaderMock = new();
        _deadReaderMock = new();
        _queueFactoryMock = new();
        _timeProvider = new();

        _walOptions = new()
        {
            ResetOnStartup = false,
        };
        
        _messageOptions = new()
        { 
            MaxDeliveryAttempts = 10 
        };

        _enqueueReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _ackReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _deadReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);

        IMessageQueue queue = new RecoveryTestMessageQueue();
        _queueFactoryMock.Setup(f => f.Create()).Returns(queue);
    }
    
    [Fact]
    public void Restore_RestoresQueueWithSingleMessage_WhenWalContainsOnlyOneEnqueueEvent()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        byte[] payload = [0x01, 0x02, 0x03];
        
        SetupWalFiles(enqueueFiles: ["enq-1"]);
        SetupEnqueueEvents("enq-1", new EnqueueWalEvent(messageId, payload));

        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();

        // Assert
        actual.Count.ShouldBe(1);
        actual.TryConsume(out Message? message);
        
        message!.Id.ShouldBe(messageId);
        message.Payload.ShouldBe(payload);
        message.DeliveryCount.ShouldBe(0);
        message.State.ShouldBe(MessageState.Restored);
    }
    
    [Fact]
    public void Restore_SetsProvidedMaxDeliveryAttempts()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        SetupWalFiles(enqueueFiles: ["enq-1"]);
        SetupEnqueueEvents("enq-1", new EnqueueWalEvent(messageId, []));
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();
        
        // Assert
        actual.TryConsume(out Message? message);
        message!.MaxDeliveryAttempts.ShouldBe(_messageOptions.MaxDeliveryAttempts);
    }

    [Fact]
    public void Restore_DoesNotChangePayload_WhenMessageIsRequeued()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        Byte[] payload = [0x01, 0x02, 0x03];

        EnqueueWalEvent[] enqueueEvents = 
        [
            new EnqueueWalEvent(messageId, payload),
            new RequeueWalEvent(messageId),
        ]; 
        
        SetupWalFiles(enqueueFiles: ["enq-1"]);
        SetupEnqueueEvents("enq-1", enqueueEvents);
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();

        // Assert
        actual.Count.ShouldBe(1);
        
        actual.TryConsume(out Message? message);
        message!.Payload.ShouldBe(payload);
    }
    
    [Fact]
    public void Restore_RestoresMessageWithSomeDeliveryCount_WhenMessageIsRequeued()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();

        EnqueueWalEvent[] enqueueEvents = 
        [
            new EnqueueWalEvent(messageId, []),
            new RequeueWalEvent(messageId),
            new RequeueWalEvent(messageId),
            new RequeueWalEvent(messageId)
        ]; 
        
        SetupWalFiles(enqueueFiles: ["enq-1"]);
        SetupEnqueueEvents("enq-1", enqueueEvents);
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();

        // Assert
        actual.Count.ShouldBe(1);
        
        actual.TryConsume(out Message? message);
        message!.DeliveryCount.ShouldBe(3);
    }

    [Fact]
    public void Recover_IgnoresRequeueEvents_WhenOriginalEnqueueEventDoesNotExist()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();

        EnqueueWalEvent[] enqueueEvents = 
        [
            new RequeueWalEvent(messageId),
            new RequeueWalEvent(messageId)
        ]; 
        
        SetupWalFiles(enqueueFiles: ["enq-1"]);
        SetupEnqueueEvents("enq-1", enqueueEvents);
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();
        
        // Assert
        actual.Count.ShouldBe(0);
        
        actual.TryConsume(out Message? message);
        message.ShouldBeNull();
    }
    
    [Fact]
    public void Recover_IgnoresMessage_WhenItIsAcked()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        
        SetupWalFiles(
            enqueueFiles: ["enq-1"], 
            ackFiles: ["ack-1"]);
        SetupEnqueueEvents("enq-1", new EnqueueWalEvent(messageId, []));
        SetupAckEvents("ack-1", new AckWalEvent(messageId));

        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();

        // Assert
        actual.Count.ShouldBe(0);
        actual.TryConsume(out Message? message);

        message.ShouldBeNull();
    }
    
    [Fact]
    public void Recover_IgnoresMessage_WhenItIsDead()
    {
        // Arrange
        Guid messageId = Guid.CreateVersion7();
        
        SetupWalFiles(
            enqueueFiles: ["enq-1"], 
            deadFiles: ["dead-1"]);
        SetupEnqueueEvents("enq-1", new EnqueueWalEvent(messageId, []));
        SetupDeadEvents("dead-1", new DeadWalEvent(messageId));

        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();

        // Assert
        actual.Count.ShouldBe(0);
        actual.TryConsume(out Message? message);

        message.ShouldBeNull();
    }

    [Fact]
    public void Recover_MaintainsFifoEnqueueOrder_WhenSomeMessagesAreRequeued()
    {
        // Arrange
        Guid messageId1 = Guid.CreateVersion7();
        Guid messageId2 = Guid.CreateVersion7();
        Guid messageId3 = Guid.CreateVersion7();

        EnqueueWalEvent[] enqueueEvents = 
        [
            new EnqueueWalEvent(messageId1, []),
            new RequeueWalEvent(messageId1),
            new EnqueueWalEvent(messageId2, []),
            new RequeueWalEvent(messageId1),
            new RequeueWalEvent(messageId1),
            new EnqueueWalEvent(messageId3, [])
        ];
        
        SetupWalFiles(enqueueFiles: ["enq-1"]);
        SetupEnqueueEvents("enq-1", enqueueEvents);
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();
        
        // Assert
        actual.Count.ShouldBe(3);
        
        actual.TryConsume(out Message? consumed1);
        consumed1!.Id.ShouldBe(messageId2);
        
        actual.TryConsume(out Message? consumed2);
        consumed2!.Id.ShouldBe(messageId1);
        
        actual.TryConsume(out Message? consumed3);
        consumed3!.Id.ShouldBe(messageId3);
    }

    [Fact]
    public void Recover_ReturnsNoMessages_WhenNoEnqueueFiles()
    {
        // Arrange
        SetupWalFiles(enqueueFiles: []);
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();
        
        // Assert
        actual.Count.ShouldBe(0);
    }

    [Fact]
    public void Restore_HandlesAllEvents_WhenEventsSpreadAcrossDifferentFiles()
    {
        // Arrange
        Guid messageId1 = Guid.CreateVersion7();
        Guid messageId2 = Guid.CreateVersion7();
        Guid messageId3 = Guid.CreateVersion7();
        Guid messageId4 = Guid.CreateVersion7();
        Guid messageId5 = Guid.CreateVersion7();
        
        SetupWalFiles(
            enqueueFiles: ["enq-1","enq-2", "enq-3", "enq-5"],
            ackFiles: ["ack-2", "ack-3"],
            deadFiles: ["dead-1", "dead-2"]);
        
        SetupEnqueueEvents(
            "enq-1", 
            new EnqueueWalEvent(messageId1, []),
            new EnqueueWalEvent(messageId2, []));
        SetupEnqueueEvents("enq-2");
        SetupEnqueueEvents(
            "enq-3", 
            new EnqueueWalEvent(messageId3, []),
            new RequeueWalEvent(messageId1),
            new EnqueueWalEvent(messageId4, []),
            new RequeueWalEvent(messageId1));
        SetupEnqueueEvents(
            "enq-5",
            new RequeueWalEvent(messageId3),
            new EnqueueWalEvent(messageId5, []));
        
        SetupAckEvents("ack-2");
        SetupAckEvents("ack-3", new AckWalEvent(messageId3));
        
        SetupDeadEvents("dead-1", new DeadWalEvent(messageId4));
        SetupDeadEvents("dead-2");
        
        RecoveryService sut = CreateRecoveryService();
        
        // Act
        IMessageQueue actual = sut.Recover();
        
        // Assert
        actual.Count.ShouldBe(3);
        
        actual.TryConsume(out Message? consumed1);
        consumed1!.Id.ShouldBe(messageId2);
        
        actual.TryConsume(out Message? consumed2);
        consumed2!.Id.ShouldBe(messageId1);
        
        actual.TryConsume(out Message? consumed3);
        consumed3!.Id.ShouldBe(messageId5);
    }
    
    private RecoveryService CreateRecoveryService()
    {
        return new RecoveryService(
            _manifestMock.Object,
            _enqueueReaderMock.Object,
            _ackReaderMock.Object,
            _deadReaderMock.Object,
            _queueFactoryMock.Object,
            _walOptions,
            _messageOptions,
            _timeProvider
        );
    }
    
    private void SetupWalFiles(
        string[]? enqueueFiles = null, 
        string[]? ackFiles = null, 
        string[]? deadFiles = null)
    {
        WalFiles files = new()
        {
            EnqueueFiles = (enqueueFiles ?? []).ToList(),
            AckFiles = (ackFiles ?? []).ToList(),
            DeadFiles = (deadFiles ?? []).ToList()
        };
        
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(files);
    }
    
    private void SetupEnqueueEvents(string file, params EnqueueWalEvent[] events)
    {
        _enqueueReaderMock.Setup(r => r.Read(file)).Returns(events);
    }

    private void SetupAckEvents(string file, params AckWalEvent[] events)
    {
        _ackReaderMock.Setup(r => r.Read(file)).Returns(events);
    }

    private void SetupDeadEvents(string file, params DeadWalEvent[] events)
    {
        _deadReaderMock.Setup(r => r.Read(file)).Returns(events);
    }
}