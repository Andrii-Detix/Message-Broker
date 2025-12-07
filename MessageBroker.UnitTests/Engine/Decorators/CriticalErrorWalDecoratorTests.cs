using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.Decorators.Wal;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.Events;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Engine.Decorators;

public class CriticalErrorWalDecoratorTests
{
    private readonly Mock<IWriteAheadLog> _innerWalMock;
    private readonly Mock<ICriticalErrorService> _criticalServiceMock;

    public CriticalErrorWalDecoratorTests()
    {
        _innerWalMock = new();
        _criticalServiceMock = new();
    }

    [Fact]
    public void Append_ShouldCallInnerWal_WhenSystemIsHealthy()
    {
        // Arrange
        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01]);
        CriticalErrorWalDecorator sut = CreateSut();

        // Act
        sut.Append(evt);
        
        // Assert
        _innerWalMock.Verify(i => i.Append(evt), Times.Once);
        _criticalServiceMock.Verify(
            c => c.Raise(It.IsAny<string>(), It.IsAny<Exception>()), 
            Times.Never);
    }

    [Fact]
    public void Append_ThrowsExceptionAndCallsShutdownService_WhenWalStorageExceptionOccurs()
    {
        // Arrange
        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01]);
        WalStorageException exception = new("Custom exception");

        _innerWalMock.Setup(i => i.Append(evt))
            .Throws(exception);
        
        CriticalErrorWalDecorator sut = CreateSut();
        
        // Act
        Action actual = () => sut.Append(evt);
        
        // Assert
        actual.ShouldThrow<WalStorageException>(exception.Message);

        _criticalServiceMock.Verify(
            c => c.Raise(It.IsAny<string>(), exception),
            Times.Once);
    }

    [Fact]
    public void Append_DoesNotCallShutdownService_WhenWalStorageExceptionDoesNotOccur()
    {
        // Arrange
        EnqueueWalEvent evt = new(Guid.CreateVersion7(), [0x01]);
        Exception exception = new("Custom exception");

        _innerWalMock.Setup(i => i.Append(evt))
            .Throws(exception);
        
        CriticalErrorWalDecorator sut = CreateSut();
        
        // Act
        Action actual = () => sut.Append(evt);
        
        // Assert
        actual.ShouldThrow<Exception>(exception.Message);

        _criticalServiceMock.Verify(
            c => c.Raise(It.IsAny<string>(), It.IsAny<Exception>()),
            Times.Never);
    }

    [Fact]
    public void Append_CallsShutdownServiceOnlyOnce_WhenWalStorageExceptionOccurs()
    {
        // Arrange
        WalStorageException exception = new("Custom exception");

        _innerWalMock.Setup(i => i.Append(It.IsAny<WalEvent>()))
            .Throws(exception);
        
        CriticalErrorWalDecorator sut = CreateSut();
        
        // Act
        Parallel.For(0, 10000, _ =>
        {
            try
            {
                sut.Append(new EnqueueWalEvent(Guid.CreateVersion7(), [0x01]));
            }
            catch { }
        });
        
        // Assert
        _criticalServiceMock.Verify(
            c => c.Raise(It.IsAny<string>(), It.IsAny<Exception>()), 
            Times.Once);
    }

    private CriticalErrorWalDecorator CreateSut()
    {
        return new(
            _innerWalMock.Object,
            _criticalServiceMock.Object);
    }
}