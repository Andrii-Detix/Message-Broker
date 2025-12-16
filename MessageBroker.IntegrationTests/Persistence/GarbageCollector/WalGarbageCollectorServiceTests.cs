using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.Manifests;
using MessageBroker.Persistence.Services.GarbageCollector;
using Moq;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.GarbageCollector;

public class WalGarbageCollectorServiceTests : IDisposable
{
    private readonly Mock<IManifestManager> _manifestMock;
    private readonly Mock<IWalReader<EnqueueWalEvent>> _enqReaderMock;
    private readonly Mock<IWalReader<AckWalEvent>> _ackReaderMock;
    private readonly Mock<IWalReader<DeadWalEvent>> _deadReaderMock;
    private readonly Mock<IFileAppenderFactory> _factoryMock;

    private readonly TestFileAppender<EnqueueWalEvent> _enqAppender;
    private readonly TestFileAppender<AckWalEvent> _ackAppender;
    private readonly TestFileAppender<DeadWalEvent> _deadAppender;

    private readonly string _directory;

    public WalGarbageCollectorServiceTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
        
        _manifestMock = new();
        _enqReaderMock = new();
        _ackReaderMock = new();
        _deadReaderMock = new();
        _factoryMock = new();

        _enqAppender = new(_directory, "merged-enq.log");
        _ackAppender = new(_directory, "merged-ack.log");
        _deadAppender = new(_directory, "merged-dead.log");

        _factoryMock.Setup(f => f.Create<EnqueueWalEvent>()).Returns(_enqAppender);
        _factoryMock.Setup(f => f.Create<AckWalEvent>()).Returns(_ackAppender);
        _factoryMock.Setup(f => f.Create<DeadWalEvent>()).Returns(_deadAppender);
        
        _enqReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _ackReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _deadReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
    }

    [Fact]
    public void Collect_FiltersOutAckedMessages()
    {
        // Arrange
        string enqFile = CreateFile("enq-1.log");
        string ackFile = CreateFile("ack-1.log");
        string activeFile = CreateFile("active.log");

        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [enqFile, activeFile],
            AckFiles = [ackFile, activeFile],
            DeadFiles = [activeFile]
        });

        Guid message1 = Guid.CreateVersion7();
        Guid message2 = Guid.CreateVersion7(); 

        _enqReaderMock.Setup(r => r.Read(enqFile)).Returns(
        [
            new EnqueueWalEvent(message1, [0x01]), 
            new EnqueueWalEvent(message2, [0x02])
        ]);

        _ackReaderMock.Setup(r => r.Read(ackFile)).Returns(
        [
            new AckWalEvent(message2) 
        ]);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        sut.Collect();

        // Assert
        _enqAppender.Verify(e => e.MessageId == message1, 1);
        
        _enqAppender.Verify(e => e.MessageId == message2, 0);

        _manifestMock.Verify(m => 
            m.Save(It.Is<WalManifest>(man => 
                man.EnqueueMerged == _enqAppender.CurrentFile && 
                man.AckMerged == string.Empty)), Times.Once);
    }

    [Fact]
    public void Collect_PreserveAcks_WhenDoNotHaveMatchedEnqueueEvent()
    {
        // Arrange
        string closedAckLog = CreateFile("ack-closed-segment.log");
        string activeLog = CreateFile("active-segment.log");

        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [activeLog], 
            AckFiles = [closedAckLog, activeLog],
            DeadFiles = [activeLog]
        });

        Guid unmatchedMessageId = Guid.CreateVersion7();
    
        _ackReaderMock.Setup(r => r.Read(closedAckLog))
            .Returns([new AckWalEvent(unmatchedMessageId)]);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        sut.Collect();

        // Assert
        _ackAppender.Verify(e => e.MessageId == unmatchedMessageId, 1);

        _manifestMock.Verify(m => m.Save(It.Is<WalManifest>(man => 
            man.AckMerged == _ackAppender.CurrentFile)), Times.Once);
    }
    
    [Fact]
    public void Collect_RemovesZombieRequeues_WhenMatchedEnqueueEventNotFound()
    {
        // Arrange
        string enqFile = CreateFile("enq-requeue.log");
        
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [enqFile, "active"], 
            AckFiles = ["active"], 
            DeadFiles = ["active"]
        });

        Guid zombieId = Guid.CreateVersion7();

        _enqReaderMock.Setup(r => r.Read(enqFile)).Returns(
        [
            new RequeueWalEvent(zombieId)            
        ]);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        sut.Collect();

        // Assert
        _enqAppender.Verify(e => e.MessageId == zombieId, 0);
    }
    
    [Fact]
    public void Collect_FiltersOutDeadMessages()
    {
        // Arrange
        string enqFile = CreateFile("enq-1.log");
        string deadFile = CreateFile("dead-1.log");
        string activeFile = CreateFile("active.log");

        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [enqFile, activeFile],
            AckFiles = [activeFile],
            DeadFiles = [deadFile, activeFile]
        });

        Guid message1 = Guid.CreateVersion7();
        Guid message2 = Guid.CreateVersion7(); 

        _enqReaderMock.Setup(r => r.Read(enqFile)).Returns(
        [
            new EnqueueWalEvent(message1, [0x01]), 
            new EnqueueWalEvent(message2, [0x02])
        ]);

        _deadReaderMock.Setup(r => r.Read(deadFile)).Returns(
        [
            new DeadWalEvent(message2) 
        ]);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        sut.Collect();

        // Assert
        _enqAppender.Verify(e => e.MessageId == message1, 1);
        
        _enqAppender.Verify(e => e.MessageId == message2, 0);

        _manifestMock.Verify(m => 
            m.Save(It.Is<WalManifest>(man => 
                man.EnqueueMerged == _enqAppender.CurrentFile && 
                man.DeadMerged == string.Empty)), Times.Once);
    }
    
    [Fact]
    public void Collect_PreservesDeadEvents_WhenDoNotHaveMatchedEnqueueEvent()
    {
        // Arrange
        string closedDeadLog = CreateFile("dead-closed-segment.log");
        string activeLog = CreateFile("active-segment.log");

        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [activeLog], 
            AckFiles = [activeLog],
            DeadFiles = [closedDeadLog, activeLog]
        });

        Guid unmatchedMessageId = Guid.CreateVersion7();
    
        _deadReaderMock.Setup(r => r.Read(closedDeadLog))
            .Returns([new DeadWalEvent(unmatchedMessageId)]);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        sut.Collect();

        // Assert
        _deadAppender.Verify(e => e.MessageId == unmatchedMessageId, 1);

        _manifestMock.Verify(m => m.Save(It.Is<WalManifest>(man => 
            man.DeadMerged == _deadAppender.CurrentFile)), Times.Once);
    }
    
    [Fact]
    public void Collect_KeepsValidRequeues_WhenMatchedEnqueueEventExists()
    {
        // Arrange
        string enqFile = CreateFile("enq-requeue.log");
        
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [enqFile, "active"], 
            AckFiles = ["active"], 
            DeadFiles = ["active"]
        });

        Guid messageId = Guid.CreateVersion7();

        _enqReaderMock.Setup(r => r.Read(enqFile)).Returns(
        [
            new EnqueueWalEvent(messageId, [0x01]),
            new RequeueWalEvent(messageId)            
        ]);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        sut.Collect();

        // Assert
        _enqAppender.Verify(e => new RequeueWalEvent(messageId) == e, 1);
    }
    
    [Fact]
    public void Collect_DeletesOnlyOldInactiveFiles_WhenCollectSucceeds()
    {
        // Arrange
        WalFiles files = new()
        {
            EnqueueFiles = CreateFiles("old-enq-merged", "enq-1", "enq-2", "enq-active").ToList(),
            AckFiles = CreateFiles("old-ack-merged", "ack-1", "ack-active").ToList(),
            DeadFiles = CreateFiles("old-dead-merged", "dead-1", "dead-active").ToList()
        };
        
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(files);
        
        string[] oldInactiveFiles = 
        [
            ..files.EnqueueFiles.SkipLast(1), 
            ..files.AckFiles.SkipLast(1), 
            ..files.DeadFiles.SkipLast(1)
        ];
        
        string[] activeFiles = 
        [
            files.EnqueueFiles.Last(),  
            files.AckFiles.Last(), 
            files.DeadFiles.Last()
        ];
        
        WalGarbageCollectorService sut = CreateSut();
        
        // Act
        sut.Collect();
        
        // Assert
        foreach (string oldInactiveFile in oldInactiveFiles)
        {
            File.Exists(oldInactiveFile).ShouldBeFalse();
        }

        foreach (string activeFile in activeFiles)
        {
            File.Exists(activeFile).ShouldBeTrue();
        }
    }
    
    [Fact]
    public void Collect_DoesNotDeleteNewMergedFiles_WhenCollectSucceeds()
    {
        // Arrange
        WalFiles files = new()
        {
            EnqueueFiles = CreateFiles("enq-1", "enq-active").ToList(),
            AckFiles = CreateFiles("ack-1", "ack-active").ToList(),
            DeadFiles = CreateFiles("dead-1", "dead-active").ToList()
        };
        
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(files);
        
        _enqReaderMock.Setup(r => r.Read(files.EnqueueFiles.First())).Returns(
        [
            new EnqueueWalEvent(Guid.CreateVersion7(), [0x01])    
        ]);
        
        _ackReaderMock.Setup(r => r.Read(files.AckFiles.First())).Returns(
        [
            new AckWalEvent(Guid.CreateVersion7())    
        ]);
        
        _deadReaderMock.Setup(r => r.Read(files.DeadFiles.First())).Returns(
        [
            new DeadWalEvent(Guid.CreateVersion7())    
        ]);
        
        WalGarbageCollectorService sut = CreateSut();
        
        // Act
        sut.Collect();
        
        // Assert
        File.Exists(_enqAppender.CurrentFile).ShouldBeTrue();
        File.Exists(_ackAppender.CurrentFile).ShouldBeTrue();
        File.Exists(_deadAppender.CurrentFile).ShouldBeTrue();
    }

    [Fact]
    public void Collect_Rollbacks_WhenExceptionOccurs()
    {
        // Arrange
        WalFiles files = new()
        {
            EnqueueFiles = CreateFiles("old-enq-merged", "enq-1", "enq-2", "enq-active").ToList(),
            AckFiles = CreateFiles("old-ack-merged", "ack-1", "ack-active").ToList(),
            DeadFiles = CreateFiles("old-dead-merged", "dead-1", "dead-active").ToList()
        };

        string[] allFiles = [..files.EnqueueFiles, ..files.AckFiles, ..files.DeadFiles];
        
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(files);
        
        _enqReaderMock.Setup(r => r.Read(files.EnqueueFiles.First())).Returns(
        [
            new EnqueueWalEvent(Guid.CreateVersion7(), [0x01])    
        ]);
        
        _ackReaderMock.Setup(r => r.Read(files.AckFiles.First())).Returns(
        [
            new AckWalEvent(Guid.CreateVersion7())    
        ]);
        
        _deadReaderMock.Setup(r => r.Read(files.DeadFiles.First())).Returns(
        [
            new DeadWalEvent(Guid.CreateVersion7())    
        ]);
        
        Exception exception = new("Custom exception");
        _enqAppender.SetExceptionOnAppend(exception);
        
        WalGarbageCollectorService sut = CreateSut();
        
        // Act
        Action actual = () => sut.Collect();
        
        // Assert
        actual.ShouldThrow<Exception>(exception.Message);

        foreach (string file in allFiles)
        {
            File.Exists(file).ShouldBeTrue();
        }
        
        File.Exists(_enqAppender.CurrentFile).ShouldBeFalse();
        File.Exists(_ackAppender.CurrentFile).ShouldBeFalse();
        File.Exists(_deadAppender.CurrentFile).ShouldBeFalse();
    }

    [Fact]
    public void Collect_CorrectlyFiltersEvents_InComplexMixedScenario()
    {
        // Arrange
        Guid messageId1 = Guid.CreateVersion7();
        Guid messageId2 = Guid.CreateVersion7();
        Guid messageId3 = Guid.CreateVersion7();
        Guid messageId4 = Guid.CreateVersion7();
        Guid messageId5 = Guid.CreateVersion7();
        Guid messageId6 = Guid.CreateVersion7();
        Guid messageId7 = Guid.CreateVersion7();
        
        WalFiles files = new()
        {
            EnqueueFiles = ["enq-1", "enq-2", "enq-3", "enq-5"],
            AckFiles = ["ack-1", "ack-2", "ack-3"],
            DeadFiles = ["dead-1", "dead-2", "dead-3"]
        };

        SetupWalFiles(files);
        
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
            new EnqueueWalEvent(messageId5, []),
            new EnqueueWalEvent(messageId6, []),
            new EnqueueWalEvent(messageId7, []));
        
        SetupAckEvents("ack-1");
        SetupAckEvents("ack-2", 
            new AckWalEvent(messageId3),
            new AckWalEvent(messageId7));
        SetupAckEvents("ack-3", new AckWalEvent(messageId5));
        
        SetupDeadEvents("dead-1", 
            new DeadWalEvent(messageId4),
            new DeadWalEvent(messageId6));
        SetupDeadEvents("dead-2");
        SetupDeadEvents("dead-3", new DeadWalEvent(messageId1));
        
        WalGarbageCollectorService sut = CreateSut();
        
        // Act
        sut.Collect();
        
        // Assert
        _enqAppender.Events.ShouldBe([
            new EnqueueWalEvent(messageId1, []),
            new EnqueueWalEvent(messageId2, []),
            new RequeueWalEvent(messageId1),
            new RequeueWalEvent(messageId1),
        ]);

        _ackAppender.Events.ShouldBe([
            new AckWalEvent(messageId7)
        ]);

        _deadAppender.Events.ShouldBe([
            new DeadWalEvent(messageId6)
        ]);
    }

    private WalGarbageCollectorService CreateSut()
    {
        return new WalGarbageCollectorService(
            _manifestMock.Object,
            _enqReaderMock.Object,
            _ackReaderMock.Object,
            _deadReaderMock.Object,
            _factoryMock.Object
        );
    }

    private string CreateFile(string fileName)
    {
        string path = Path.Combine(_directory, fileName);
        File.Create(path).Dispose();
        return path;
    }

    private string[] CreateFiles(params string[] fileNames)
    {
        List<string> paths = [];
        
        foreach (string fileName in fileNames)
        {
            string path = CreateFile(fileName);
            paths.Add(path);
        }
        
        return paths.ToArray();
    }
    
    private void SetupEnqueueEvents(string file, params EnqueueWalEvent[] events)
    {
        _enqReaderMock.Setup(r => r.Read(file)).Returns(events);
    }

    private void SetupAckEvents(string file, params AckWalEvent[] events)
    {
        _ackReaderMock.Setup(r => r.Read(file)).Returns(events);
    }

    private void SetupDeadEvents(string file, params DeadWalEvent[] events)
    {
        _deadReaderMock.Setup(r => r.Read(file)).Returns(events);
    }
    
    private void SetupWalFiles(WalFiles files)
    {
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(files);
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}