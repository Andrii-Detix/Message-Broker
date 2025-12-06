using MessageBroker.Persistence.Extensions;
using Shouldly;

namespace MessageBroker.UnitTests.Persistence.Extensions;

public class WalFormatExtensionsTests
{
    [Fact]
    public void OrderByWalFormat_SortsByDateChronologically()
    {
        // Arrange
        string file1 = "queue-20250101120000-1.log";
        string file2 = "queue-20250102120000-1.log";
        
        string[] input = [file2, file1];

        // Act
        string[] actual = input.OrderByWalFormat().ToArray();

        // Assert
        actual.Length.ShouldBe(2);
        actual[0].ShouldBe(file1);
        actual[1].ShouldBe(file2);
    }

    [Fact]
    public void OrderByWalFormat_SortsBySequenceNumerically_NotAlphabetically()
    {
        // Arrange
        string file2 = "queue-20250101120000-2.log";
        string file10 = "queue-20250101120000-10.log"; 
        
        string[] input = [file10, file2]; 

        // Act
        string[] actual = input.OrderByWalFormat().ToArray();

        // Assert
        actual.Length.ShouldBe(2);
        actual[0].ShouldBe(file2);
        actual[1].ShouldBe(file10);
    }

    [Fact]
    public void OrderByWalFormat_HandlesComplexNamesWithHyphens()
    {
        // Arrange
        string file = "my-super-queue-20250101120000-1.log";
        string[] input = [file];

        // Act
        string[] actual = input.OrderByWalFormat().ToArray();

        // Assert
        actual.Length.ShouldBe(1);
        actual[0].ShouldBe(file);
    }

    [Fact]
    public void OrderByWalFormat_IgnoresFilesWithInvalidFormat()
    {
        // Arrange
        string valid = "queue-20250101120000-1.log";
        string invalidDate = "queue-notadate-1.log";
        string invalidSeq = "queue-20250101120000-notaseq.log";
        string garbage = "somerandomfile.txt";
        string wrongDateFormat = "queue-2025-01-01-1.log";

        string[] input = [valid, invalidDate, invalidSeq, garbage, wrongDateFormat];

        // Act
        string[] actual = input.OrderByWalFormat().ToArray();

        // Assert
        actual.Length.ShouldBe(1);
        actual[0].ShouldBe(valid);
    }

    [Fact]
    public void OrderByWalFormat_HandlesFullPaths()
    {
        // Arrange
        string path1 = "/var/data/wal/queue-20250101120000-1.log";
        string path2 = "C:\\Logs\\queue-20250101120000-2.log";
        
        string[] input = [path2, path1];

        // Act
        string[] actual = input.OrderByWalFormat().ToArray();

        // Assert
        actual.Length.ShouldBe(2);
        actual[0].ShouldBe(path1);
        actual[1].ShouldBe(path2);
    }

    [Fact]
    public void OrderByWalFormat_ReturnsEmpty_WhenInputIsEmpty()
    {
        // Arrange
        var input = Array.Empty<string>();

        // Act
        var actual = input.OrderByWalFormat();

        // Assert
        actual.ShouldBeEmpty();
    }
    
    [Fact]
    public void OrderByWalFormat_SortsByTimestamp_ThenBySequenceNumber()
    {
        // Arrange
        string fileDay1Seq1 = "queue-20250101120000-1.log";
        string fileDay1Seq2 = "queue-20250101120000-2.log";
        string fileDay1Seq10 = "queue-20250101120000-10.log";
        string fileDay2Seq1 = "queue-20250102120000-1.log";

        string[] input =
        [
            fileDay1Seq10, 
            fileDay2Seq1, 
            fileDay1Seq1, 
            fileDay1Seq2
        ];

        // Act
        string[] actual = input.OrderByWalFormat().ToArray();

        // Assert
        actual.Length.ShouldBe(4);

        actual[0].ShouldBe(fileDay1Seq1);
        actual[1].ShouldBe(fileDay1Seq2);
        actual[2].ShouldBe(fileDay1Seq10);
        actual[3].ShouldBe(fileDay2Seq1);
    }
}