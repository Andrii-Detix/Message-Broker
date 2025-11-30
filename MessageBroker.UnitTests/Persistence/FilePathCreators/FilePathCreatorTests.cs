using MessageBroker.Persistence.FilePathCreators;
using MessageBroker.Persistence.FilePathCreators.Exceptions;
using Microsoft.Extensions.Time.Testing;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.FilePathCreators;

public class FilePathCreatorTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_ThrowsException_WhenPrefixIsEmpty(string? prefix)
    {
        // Arrange
        string directory = "dir1";
        string extension = "ext";
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new FilePathCreator(directory, prefix, extension, timeProvider);
        
        // Assert
        actual.ShouldThrow<FileNamePrefixEmptyException>();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_ThrowsException_WhenFileExtensionIsEmpty(string? extension)
    {
        // Arrange
        string directory = "dir1";
        string prefix = "prefix";
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new FilePathCreator(directory, prefix, extension, timeProvider);
        
        // Assert
        actual.ShouldThrow<FileNameExtensionEmptyException>();
    }

    [Theory]
    [InlineData(".")]
    [InlineData("  . ")]
    [InlineData("..")]
    public void Constructor_ThrowsException_WhenFileExtensionContainsOnlyDots(string extension)
    {
        // Arrange
        string directory = "dir1";
        string prefix = "prefix";
        FakeTimeProvider timeProvider = new();
        
        // Act
        Action actual = () => new FilePathCreator(directory, prefix, extension, timeProvider);
        
        // Assert
        actual.ShouldThrow<FileNameExtensionEmptyException>();
    }
    
    [Fact]
    public void Constructor_ThrowsException_WhenTimeProviderIsNull()
    {
        // Arrange
        string directory = "dir1";
        string prefix = "prefix";
        string extension = "ext";
        
        // Act
        Action actual = () => new FilePathCreator(directory, prefix, extension, null);
        
        // Assert
        actual.ShouldThrow<ArgumentNullException>();
    }
    
    [Fact]
    public void Constructor_CreatesFilePathCreator_WhenProvidedDataIsValid()
    {
        // Arrange
        string directory = "dir1";
        string prefix = "prefix";
        string extension = "ext";
        FakeTimeProvider timeProvider = new();
        
        // Act
        FilePathCreator actual = new(directory, prefix, extension, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_CreatesFilePathCreator_WhenProvidedDirectoryIsEmpty(string? directory)
    {
        // Arrange
        string prefix = "prefix";
        string extension = "ext";
        FakeTimeProvider timeProvider = new();
        
        // Act
        FilePathCreator actual = new(directory, prefix, extension, timeProvider);
        
        // Assert
        actual.ShouldNotBeNull();
    }

    [Fact]
    public void CreatePath_ReturnsCorrectPath_WhenMethodCallIsFirst()
    {
        // Arrange
        DateTimeOffset time = new(2025, 11, 28, 15, 30, 0, TimeSpan.Zero);
        FakeTimeProvider timeProvider = new(time);
        FilePathCreator sut = new("dir1", "prefix", "ext", timeProvider);
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        string? actualDirectory = Path.GetDirectoryName(actual);
        string actualFileName = Path.GetFileName(actual);
        actualDirectory.ShouldNotBeNull();
        actualDirectory.ShouldBe("dir1");
        actualFileName.ShouldNotBeNull();
        actualFileName.ShouldBe("prefix-20251128153000-1.ext");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void CreatePath_ReturnsPathWithoutDirectory_WhenInputDirectoryIsEmpty(string? directory)
    {
        // Arrange
        DateTimeOffset time = new(2025, 11, 28, 15, 30, 0, TimeSpan.Zero);
        FakeTimeProvider timeProvider = new(time);
        FilePathCreator sut = new(directory, "prefix", "ext", timeProvider);
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        string? actualDirectory = Path.GetDirectoryName(actual);
        string actualFileName = Path.GetFileName(actual);
        actualDirectory.ShouldNotBeNull();
        actualDirectory.ShouldBe(string.Empty);
        actualFileName.ShouldNotBeNull();
        actualFileName.ShouldBe("prefix-20251128153000-1.ext");
    }

    [Fact]
    public void CreatePath_ReturnsPathWithOnlyOneDotExtensionSeparator_WhenInputExtensionHasLeadedDot()
    {
        // Arrange
        DateTimeOffset time = new(2025, 11, 28, 15, 30, 0, TimeSpan.Zero);
        FakeTimeProvider timeProvider = new(time);
        FilePathCreator sut = new("dir1", "prefix", ".ext", timeProvider);
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        string actualFileName = Path.GetFileName(actual);
        actualFileName.ShouldNotBeNull();
        actualFileName.Count(c => c == '.').ShouldBe(1);
        actualFileName.ShouldEndWith(".ext");
    }

    [Fact]
    public void CreatePath_IncrementSegmentNumber_WhenTheSecondCallWasAtTheSameSecondWithFirstCall()
    {
        // Arrange
        DateTimeOffset time = new(2025, 11, 28, 15, 30, 0, TimeSpan.Zero);
        FakeTimeProvider timeProvider = new(time);
        FilePathCreator sut = new("dir1", "prefix", "ext", timeProvider);

        sut.CreatePath();
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        string actualFileName = Path.GetFileName(actual);
        actualFileName.ShouldNotBeNull();
        actualFileName.ShouldContain("20251128153000");
        actualFileName.ShouldEndWith("-2.ext");
    }

    [Fact]
    public void CreatePath_ContinuesIncrementingSegmentNumber_WhenMethodIsCalledSeveralTimesAtTheSameSecond()
    {
        // Arrange
        DateTimeOffset time = new(2025, 11, 28, 15, 30, 0, TimeSpan.Zero);
        FakeTimeProvider timeProvider = new(time);
        FilePathCreator sut = new("dir1", "prefix", "ext", timeProvider);

        sut.CreatePath();
        
        timeProvider.Advance(TimeSpan.FromSeconds(0.6));
        sut.CreatePath();
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        string actualFileName = Path.GetFileName(actual);
        actualFileName.ShouldNotBeNull();
        actualFileName.ShouldContain("20251128153000");
        actualFileName.ShouldEndWith("-3.ext");
    }
    
    [Fact]
    public void CreatePath_DropsSegmentNumberAndUpdateCreationTime_WhenTheSecondCallWasAtTheNextSecondButDifferenceBetweenCallsLessThanOneSecond()
    {
        // Arrange
        DateTimeOffset time = new(2025, 11, 28, 15, 30, 0, TimeSpan.Zero);
        FakeTimeProvider timeProvider = new(time);
        FilePathCreator sut = new("dir1", "prefix", "ext", timeProvider);
        
        timeProvider.Advance(TimeSpan.FromSeconds(0.9));
        sut.CreatePath();
        
        timeProvider.Advance(TimeSpan.FromSeconds(0.1));
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        string actualFileName = Path.GetFileName(actual);
        actualFileName.ShouldNotBeNull();
        actualFileName.ShouldContain("20251128153001");
        actualFileName.ShouldEndWith("-1.ext");
    }
}