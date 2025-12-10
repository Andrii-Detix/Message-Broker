using MessageBroker.Persistence.FilePathCreators;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.FilePathCreators;

public class FixedFilePathCreatorTests
{
    [Fact]
    public void CreatePath_ReturnsInputFilePath()
    {
        // Arrange
        FixedFilePathCreator sut = new("path.log");
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBe("path.log");
    }

    [Fact]
    public void CreatePath_ReturnsFixedFilePath_WhenCallsMethodSeveralTimes()
    {
        // Arrange
        FixedFilePathCreator sut = new("path.log");
        
        sut.CreatePath();
        sut.CreatePath();
        sut.CreatePath();
        
        // Act
        string actual = sut.CreatePath();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBe("path.log");
    }
}