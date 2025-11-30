using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.FileAppenders.Exceptions;
using MessageBroker.Persistence.FilePathCreators;
using Microsoft.Extensions.Time.Testing;
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
    public void Constructor_ThrowsException_WhenFilePathCreatorIsNull()
    {
        // Arrange
        int maxWriteCountPerFile = 1;
        
        // Act
        Action actual = () => new TestFileAppender(null, maxWriteCountPerFile);
        
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
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        
        // Act
        Action actual = () => new TestFileAppender(pathCreator, maxWriteCountPerFile);
        
        // Assert
        actual.ShouldThrow<MaxWriteCountPerFileInvalidException>();
    }

    [Fact]
    public void Constructor_CreatesFileAppender_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);

        // Act
        using TestFileAppender actual = new(pathCreator, 1);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void Constructor_CreatesFileForStoringData_WhenInputDataIsValid()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);

        // Act
        TestFileAppender actual = new(pathCreator, 1);
        
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
        IFilePathCreator pathCreator = new FilePathCreator(dir, "prefix", "ext", timeProvider);

        // Act
        using TestFileAppender actual = new(pathCreator, 1);
        
        // Assert
        Directory.Exists(dir).ShouldBeTrue();
    }

    [Fact]
    public void SaveData_WritesDataToFile_WhenInputDataIsNotEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 3);
        
        // Act
        sut.WriteRawBytes([0x01, 0x02]);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(2);
        content.ShouldBe([0x01, 0x02]);
    }

    [Fact]
    public void SaveData_AppendsDataToTheEndOfFile_WhenSomeDataIsAlreadyWritten()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 3);
        
        sut.WriteRawBytes([0x01, 0x02]);
        
        // Act
        sut.WriteRawBytes([0x01, 0x03]);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(4);
        content.ShouldBe([0x01, 0x02, 0x01, 0x03]);
    }

    [Fact]
    public void SavaData_RotatesFileAndSaveDataIntoIt_WhenMaxWriteCountPerFileWasReached()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 1);
        
        string file1 = sut.CurrentFile;
        sut.WriteRawBytes([0x01, 0x02]);
        
        // Act
        sut.WriteRawBytes([0x01, 0x03]);
        
        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        actualFile.ShouldNotBe(file1);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(2);
        content.ShouldBe([0x01, 0x03]);
    }

    [Fact]
    public void SaveData_StoresAllData_WhenWritesDataConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 100000);
        
        int threadCount = 10;
        int writesPerThread = 1000;
        
        // Act
        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < writesPerThread; i++)
            {
                sut.WriteRawBytes([0x01, 0x02]);
            }
        });

        // Assert
        string actualFile = sut.CurrentFile;
        sut.Dispose();
        
        byte[] content = File.ReadAllBytes(actualFile);
        content.ShouldNotBeEmpty();
        content.Length.ShouldBe(threadCount * writesPerThread * 2);

        for (int i = 0; i < content.Length; i += 2)
        {
            content[i].ShouldBe((byte)0x01);
            content[i + 1].ShouldBe((byte)0x02);
        }
    }

    [Fact]
    public void SaveData_HandlesFileRotation_WhenWritesDataConcurrently()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 1000);

        int threadCount = 10;
        int writesPerThread = 1000;
        
        // Act
        Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < writesPerThread; i++)
            {
                sut.WriteRawBytes([0x01, 0x02]);
            }
        });
        
        // Assert
        sut.Dispose();

        string[] files = Directory.GetFiles(_directory, "*.ext");
        files.Length.ShouldBe(10);
        
        byte[][] content = files.Select(File.ReadAllBytes).ToArray();
        foreach (var fileContent in content)
        {
            fileContent.Length.ShouldBe(2000);
        }
    }

    [Fact]
    public void SaveData_ThrowsException_WhenFileAppenderIsDisposed()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 3);
        sut.Dispose();
        
        // Act
        Action actual = () => sut.WriteRawBytes([0x01, 0x02]);
        
        // Assert
        actual.ShouldThrow<FileAppenderDisposedException>();
    }

    [Fact]
    public void Dispose_SetCurrentFileAsEmpty()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        IFilePathCreator pathCreator = new FilePathCreator(_directory, "prefix", "ext", timeProvider);
        TestFileAppender sut = new(pathCreator, 3);
        
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