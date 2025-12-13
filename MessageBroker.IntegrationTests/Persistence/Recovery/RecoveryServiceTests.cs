using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.Manifests;
using MessageBroker.Persistence.Services.Recovery;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.Recovery;

public class RecoveryServiceTests : IDisposable
{
    private readonly string _directory;
    
    private readonly Mock<IManifestManager> _manifestMock;
    private readonly Mock<IWalReader<EnqueueWalEvent>> _enqueueReaderMock;
    private readonly Mock<IWalReader<AckWalEvent>> _ackReaderMock;
    private readonly Mock<IWalReader<DeadWalEvent>> _deadReaderMock;
    private readonly Mock<IMessageQueueFactory> _queueFactoryMock;
    private readonly FakeTimeProvider _timeProvider;
    private readonly MessageOptions _messageOptions;

    public RecoveryServiceTests()
    {
        string directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(directory);
        _directory = directory;
        
        _manifestMock = new();
        _enqueueReaderMock = new();
        _ackReaderMock = new();
        _deadReaderMock = new();
        _queueFactoryMock = new();
        _timeProvider = new();
        _messageOptions = new();
        
        _manifestMock.Setup(m => m.LoadWalFiles())
            .Returns(new WalFiles 
            { 
                EnqueueFiles = [], 
                AckFiles = [], 
                DeadFiles = []
            });
        
        _enqueueReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _ackReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        _deadReaderMock.Setup(r => r.Read(It.IsAny<string>())).Returns([]);
        
        IMessageQueue queue = new RecoveryTestMessageQueue();
        _queueFactoryMock.Setup(f => f.Create()).Returns(queue);
    }

    [Fact]
    public void Recover_PhysicallyDeletesOldFiles_WhenResetOnStartupIsTrue()
    {
        // Arrange
        string enqueueFile = Path.Combine(_directory, "enq.log");
        string nestedDir = Path.Combine(_directory, "nested");
        string nestedFile = Path.Combine(nestedDir, "nested-file.log");
        
        Directory.CreateDirectory(nestedDir);
        File.Create(enqueueFile).Dispose();
        File.Create(nestedFile).Dispose();

        WalOptions walOptions = new()
        {
            Directory = _directory,
            ResetOnStartup = true
        };

        RecoveryService sut = CreateSut(walOptions);
        
        // Act
        sut.Recover();
        
        // Assert
        Directory.Exists(nestedDir).ShouldBeFalse();
        File.Exists(enqueueFile).ShouldBeFalse();
        File.Exists(nestedFile).ShouldBeFalse();
        
        Directory.GetFiles(_directory, "*", SearchOption.AllDirectories).ShouldBeEmpty();
    }

    [Fact]
    public void Recover_KeepsFiles_WhenResetOnStartupIsFalse()
    {
        // Arrange
        string enqueueFile = Path.Combine(_directory, "enq.log");
        string nestedDir = Path.Combine(_directory, "nested");
        string nestedFile = Path.Combine(nestedDir, "nested-file.log");
        
        Directory.CreateDirectory(nestedDir);
        File.Create(enqueueFile).Dispose();
        File.Create(nestedFile).Dispose();

        WalOptions walOptions = new()
        {
            Directory = _directory,
            ResetOnStartup = false
        };

        RecoveryService sut = CreateSut(walOptions);
        
        // Act
        sut.Recover();
        
        // Assert
        Directory.Exists(nestedDir).ShouldBeTrue();
        File.Exists(enqueueFile).ShouldBeTrue();
        File.Exists(nestedFile).ShouldBeTrue();
    }
    
    private RecoveryService CreateSut(WalOptions walOptions)
    {
        return new RecoveryService(
            _manifestMock.Object,
            _enqueueReaderMock.Object,
            _ackReaderMock.Object,
            _deadReaderMock.Object,
            _queueFactoryMock.Object,
            walOptions,
            _messageOptions,
            _timeProvider
        );
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}