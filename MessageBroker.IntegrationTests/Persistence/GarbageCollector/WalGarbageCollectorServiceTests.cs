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

    private readonly Mock<IFileAppender<EnqueueWalEvent>> _enqAppenderMock;
    private readonly Mock<IFileAppender<AckWalEvent>> _ackAppenderMock;
    private readonly Mock<IFileAppender<DeadWalEvent>> _deadAppenderMock;

    private readonly string _directory;

    public WalGarbageCollectorServiceTests()
    {
        _manifestMock = new();
        _enqReaderMock = new();
        _ackReaderMock = new();
        _deadReaderMock = new();
        _factoryMock = new();

        _enqAppenderMock = new();
        _ackAppenderMock = new();
        _deadAppenderMock = new();

        _factoryMock.Setup(f => f.Create<EnqueueWalEvent>()).Returns(_enqAppenderMock.Object);
        _factoryMock.Setup(f => f.Create<AckWalEvent>()).Returns(_ackAppenderMock.Object);
        _factoryMock.Setup(f => f.Create<DeadWalEvent>()).Returns(_deadAppenderMock.Object);

        _enqAppenderMock.Setup(a => a.CurrentFile).Returns("merged-enq.log");
        _ackAppenderMock.Setup(a => a.CurrentFile).Returns("merged-ack.log");
        _deadAppenderMock.Setup(a => a.CurrentFile).Returns("merged-dead.log");
        
        _enqReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _ackReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _deadReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
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
        _enqAppenderMock.Verify(a => 
            a.Append(It.Is<EnqueueWalEvent>(e => e.MessageId == message1)), Times.Once);
        
        _enqAppenderMock.Verify(a => 
            a.Append(It.Is<EnqueueWalEvent>(e => e.MessageId == message2)), Times.Never);

        _manifestMock.Verify(m => 
            m.Save(It.Is<WalManifest>(man => 
                man.EnqueueMerged == "merged-enq.log" && 
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
        _ackAppenderMock.Verify(a => 
            a.Append(It.Is<AckWalEvent>(e => e.MessageId == unmatchedMessageId)), Times.Once);

        _manifestMock.Verify(m => m.Save(It.Is<WalManifest>(man => 
            man.AckMerged == "merged-ack.log")), Times.Once);
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
        _enqAppenderMock.Verify(a => 
            a.Append(It.Is<RequeueWalEvent>(e => e.MessageId == zombieId)), Times.Never);
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
        _enqAppenderMock.Verify(a => 
            a.Append(It.Is<EnqueueWalEvent>(e => e.MessageId == message1)), Times.Once);
        
        _enqAppenderMock.Verify(a => 
            a.Append(It.Is<EnqueueWalEvent>(e => e.MessageId == message2)), Times.Never);

        _manifestMock.Verify(m => 
            m.Save(It.Is<WalManifest>(man => 
                man.EnqueueMerged == "merged-enq.log" && 
                man.DeadMerged == string.Empty)), Times.Once);
    }
    
    [Fact]
    public void Collect_PreservsDeadEvents_WhenDoNotHaveMatchedEnqueueEvent()
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
        _deadAppenderMock.Verify(a => 
            a.Append(It.Is<DeadWalEvent>(e => e.MessageId == unmatchedMessageId)), Times.Once);

        _manifestMock.Verify(m => m.Save(It.Is<WalManifest>(man => 
            man.DeadMerged == "merged-dead.log")), Times.Once);
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
        _enqAppenderMock.Verify(a => 
            a.Append(It.Is<RequeueWalEvent>(e => e.MessageId == messageId)), Times.Once);
    }

    [Fact]
    public void Collect_Rollbacks_WhenExceptionOccurs()
    {
        // Arrange
        string enqFile = CreateFile("enq-error.log");
        _manifestMock.Setup(m => m.LoadWalFiles()).Returns(new WalFiles
        {
            EnqueueFiles = [enqFile, "active"], 
            AckFiles = ["active"], 
            DeadFiles = ["active"]
        });
        
        Guid messageId = Guid.CreateVersion7();

        _enqReaderMock.Setup(r => r.Read(enqFile)).Returns(
        [
            new EnqueueWalEvent(messageId, [0x01])    
        ]);

        Exception exception = new("Custom exception");
        _enqAppenderMock.Setup(a => a.Append(It.IsAny<EnqueueWalEvent>()))
            .Throws(exception);

        WalGarbageCollectorService sut = CreateSut();

        // Act
        Action actual = () => sut.Collect();
        
        // Assert
        actual.ShouldThrow<Exception>(exception.Message);

        File.Exists(enqFile).ShouldBeTrue();
        
        _manifestMock.Verify(x => x.Save(It.IsAny<WalManifest>()), Times.Never);
    }

    [Fact]
    public void Collect_DeletesOldInactiveFiles_WhenCollectSucceeds()
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
    public void Collect_PreservesFiles_WhenExceptionOccurs()
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
        
        Exception exception = new("Custom exception");
        _enqAppenderMock.Setup(a => a.Append(It.IsAny<EnqueueWalEvent>()))
            .Throws(exception);
        
        WalGarbageCollectorService sut = CreateSut();
        
        // Act
        Action actual = () => sut.Collect();
        
        // Assert
        actual.ShouldThrow<Exception>(exception.Message);

        foreach (string file in allFiles)
        {
            File.Exists(file).ShouldBeTrue();
        }
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

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}