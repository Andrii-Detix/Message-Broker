using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.WalReaders;

public class AbstractWalReaderTests : IDisposable
{
    private readonly string _directory;

    public AbstractWalReaderTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);
    }

    [Fact]
    public void Read_ReturnsAllEvents_WhenFileIsValid()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        
        using (BinaryWriter writer = new(File.OpenWrite(filePath)))
        {
            writer.Write(10);
            writer.Write(20);
            writer.Write(30);
        }
        
        TestWalReader sut = new();
        
        // Act
        TestWalEvent[] actual = sut.Read(filePath).ToArray();
        
        // Assert
        actual.Length.ShouldBe(3);
        actual[0].Value.ShouldBe(10);
        actual[1].Value.ShouldBe(20);
        actual[2].Value.ShouldBe(30);
    }
    
    [Fact]
    public void Read_ReturnsEmpty_WhenFileDoesNotExist()
    {
        // Arrange
        TestWalReader sut = new();
        string filePath = Path.Combine(_directory, "empty.log");

        // Act
        TestWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.ShouldBeEmpty();
    }

    [Fact]
    public void Read_ReturnsAllCorrectWalEvents_WhenFileIsCorruptedAtTheEnd()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            writer.Write(10);
            writer.Write((byte)1);
        }

        TestWalReader sut = new();

        // Act
        TestWalEvent[] actual = sut.Read(filePath).ToArray();

        // Assert
        actual.Length.ShouldBe(1);
        actual[0].Value.ShouldBe(10);
    }

    [Fact]
    public void Read_ThrowsException_WhenUnexpectedExceptionIsThrownDuringExecution()
    {
        // Arrange
        string filePath = Path.Combine(_directory, "valid.log");
        File.Create(filePath).Dispose();
        
        using (var writer = new BinaryWriter(File.OpenWrite(filePath)))
        {
            writer.Write(10);
        }
        
        FaultyTestWalReader sut = new();
        
        // Act
        Action actual = () => sut.Read(filePath).ToArray();
        
        // Assert
        actual.ShouldThrow<CustomException>();
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}