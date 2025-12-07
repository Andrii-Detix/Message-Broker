using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Common.Exceptions;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.FileAppenders.Exceptions;
using MessageBroker.Persistence.FilePathCreators;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.FileAppenders;

public class AbstractFileAppenderTests : IDisposable
{
    private readonly string _directory;

    public AbstractFileAppenderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
    }

    [Fact]
    public void Constructor_ThrowsException_WhenCrcProviderIsNull()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        int maxWriteCountPerFile = 1;
        
        // Act
        Action actual = () => new TestFileAppender(null, pathCreator, maxWriteCountPerFile);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenFilePathCreatorIsNull()
    {
        // Arrange
        ICrcProvider crcProvider = new CrcProvider();
        int maxWriteCountPerFile = 1;
        
        // Act
        Action actual = () => new TestFileAppender(crcProvider, null, maxWriteCountPerFile);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_ThrowsException_WhenMaxWriteCountPerFileIsLessThanOne(int maxWriteCountPerFile)
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        
        // Act
        Action actual = () => new TestFileAppender(crcProvider, pathCreator, maxWriteCountPerFile);
        
        // Assert
        actual.ShouldThrow<MaxWriteCountPerFileInvalidException>();
    }

    [Fact]
    public void Constructor_CreatesFileAppender_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);

        // Act
        using TestFileAppender actual = new(crcProvider, pathCreator, 1);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Constructor_CreatesFileForStoringData_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);

        // Act
        TestFileAppender actual = new(crcProvider, pathCreator, 1);
        
        // Assert
        string filePath = actual.CurrentFile;
        actual.Dispose();
        
        filePath.ShouldEndWith("-1.ext");
        File.Exists(filePath).ShouldBeTrue();
    }

    [Fact]
    public void Constructor_CreatesDirectory_WhenDirectoryDoesNotExist()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        string dir = Path.Combine(_directory, Path.GetRandomFileName());
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(dir, "prefix", "ext", timeProvider);

        // Act
        using TestFileAppender actual = new(crcProvider, pathCreator, 1);
        
        // Assert
        Directory.Exists(dir).ShouldBeTrue();
    }

    [Fact]
    public void SaveData_WritesDataToFile_WhenInputDataIsNotEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 3);

        byte[] data = [0x01, 0x02];
        byte[] header = new byte[8];
        crcProvider.WriteHeader(header, data);
        int expectedSize = 8 + 2; // Header (8) + Data (2) = 10
        
        // Act
        sut.WriteRawBytes(data);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedSize);
        
        byte[] actualHeader = content.AsSpan(0, 8).ToArray();
        byte[] actualData = content.AsSpan(8).ToArray();
        
        actualHeader.ShouldBe(header);
        actualData.ShouldBe(data);
    }

    [Fact]
    public void SaveData_AppendsDataToTheEndOfFile_WhenSomeDataIsAlreadyWritten()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 3);
        
        byte[] data1 = [0x01, 0x02];
        byte[] header1 = new byte[8];
        crcProvider.WriteHeader(header1, data1);
        
        byte[] data2 = [0x01, 0x03];
        byte[] header2 = new byte[8];
        crcProvider.WriteHeader(header2, data2);
        
        byte[] expectedBuffer = header1.Concat(data1).Concat(header2).Concat(data2).ToArray();
        int expectedSize = 8 + 2 + 8 + 2; // Header (8) + Data (2) + Header (8) + Data (2) = 20
        
        sut.WriteRawBytes(data1);
        
        // Act
        sut.WriteRawBytes(data2);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedSize);
        content.ShouldBe(expectedBuffer);
    }

    [Fact]
    public void SaveData_RotatesFileAndSaveDataIntoIt_WhenMaxWriteCountPerFileWasReached()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 1);
        
        byte[] data = [0x01, 0x03];
        byte[] header = new byte[8];
        crcProvider.WriteHeader(header, data);

        byte[] expectedBuffer = header.Concat(data).ToArray();
        int expectedSize = 8 + 2; // Header (8) + Data (2) = 10
        
        string file1 = sut.CurrentFile;
        sut.WriteRawBytes([0x01, 0x02]);
        
        // Act
        sut.WriteRawBytes(data);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        actualFile.ShouldNotBe(file1);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(expectedSize);
        content.ShouldBe(expectedBuffer);
    }

    [Fact]
    public void SaveData_StoresAllData_WhenWritesDataConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 100000);

        byte[] data = [0x01, 0x02];
        byte[] header = new byte[8];
        crcProvider.WriteHeader(header, data);
        int writeSize = 8 + 2; // Header (8) + Data (2) = 10
        byte[] expectedBuffer = header.Concat(data).ToArray();
        
        int threadCount = 10;
        int writesPerThread = 1000;
        
        // Act
        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < writesPerThread; i++)
            {
                sut.WriteRawBytes(data);
            }
        });

        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(threadCount * writesPerThread * writeSize);

        for (int i = 0; i < content.Length; i += writeSize)
        {
            byte[] actualBuffer = content.AsSpan(i, writeSize).ToArray();
            actualBuffer.ShouldBe(expectedBuffer);
        }
    }

    [Fact]
    public void SaveData_HandlesFileRotation_WhenWritesDataConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 1000);

        byte[] data = [0x01, 0x02];
        int writeSize = 8 + 2; // Header (8) + Data (2) = 10
        
        int threadCount = 10;
        int writesPerThread = 1000;
        
        // Act
        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < writesPerThread; i++)
            {
                sut.WriteRawBytes(data);
            }
        });
        
        // Assert
        sut.Dispose();

        string[] files = Directory.GetFiles(_directory, "*.ext");
        files.Length.ShouldBe(10);
        
        byte[][] content = files.Select(File.ReadAllBytes).ToArray();
        foreach (var fileContent in content)
        {
            fileContent.Length.ShouldBe(1000 * writeSize);
        }
    }

    [Fact]
    public void SaveData_ThrowsException_WhenFileAppenderIsDisposed()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 3);
        sut.Dispose();
        
        // Act
        Action actual = () => sut.WriteRawBytes([0x01, 0x02]);
        
        // Assert
        actual.ShouldThrow<FileAppenderDisposedException>();
    }
    
    [Fact]
    public void SaveData_RetriesAndSucceeds_WhenFirstRotationFails()
    {
        // Arrange
        CrcProvider crcProvider = new();
        Mock<IFilePathCreator> pathCreatorMock = new();

        string initialFile = Path.Combine(_directory, "start.log");
        string badPath = Path.Combine(_directory, "bad|<>.log");
        string recoveryFile = Path.Combine(_directory, "recovery.log");

        pathCreatorMock.SetupSequence(pc => pc.CreatePath())
            .Returns(initialFile)
            .Returns(badPath)
            .Returns(recoveryFile);
        
        TestFileAppender sut = new(crcProvider, pathCreatorMock.Object, 1);
    
        sut.WriteRawBytes([0xAA]); 

        // Act
        sut.WriteRawBytes([0xBB]); 

        // Assert
        string currentFile = sut.CurrentFile;
        sut.Dispose();
    
        currentFile.ShouldBe(recoveryFile);
        File.Exists(recoveryFile).ShouldBeTrue();
    
        File.ReadAllBytes(recoveryFile).Length.ShouldBe(9); // Header (8) + Data (1)
    }
    
    [Fact]
    public void SaveData_DisposesAppenderAndThrowsException_WhenRetryAlsoFails()
    {
        // Arrange
        CrcProvider crcProvider = new();
        Mock<IFilePathCreator> pathCreatorMock = new();

        string initialFile = Path.Combine(_directory, "start.log");
        string badPath1 = Path.Combine(_directory, "bad1|<>.log");
        string badPath2 = Path.Combine(_directory, "bad2|<>.log");

        pathCreatorMock.SetupSequence(x => x.CreatePath())
            .Returns(initialFile)
            .Returns(badPath1)
            .Returns(badPath2);
        
        TestFileAppender sut = new(crcProvider, pathCreatorMock.Object, 1);
        sut.WriteRawBytes([0xAA]);

        // Act
        Action actual = () => sut.WriteRawBytes([0xBB]);

        // Assert
        actual.ShouldThrow<WalStorageException>();
    
        Should.Throw<FileAppenderDisposedException>(() => sut.WriteRawBytes([0xCC]));
    }
    
    [Fact]
    public void Dispose_SetCurrentFileAsEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        ICrcProvider crcProvider = new CrcProvider();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(crcProvider, pathCreator, 3);
        
        // Act
        sut.Dispose();
        
        // Assert
        sut.CurrentFile.ShouldBeEmpty();
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}