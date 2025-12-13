using FsCheck;
using FsCheck.Fluent;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.FilePathCreators;
using MessageBroker.Persistence.WalReaders.Exceptions;

namespace MessageBroker.IntegrationTests.Persistence.WalStorage;

public class WalStoragePropertyTests
{
    [FsCheck.Xunit.Property]
    public Property AbstractWal_PersistsAndRestoresCorrectly()
    {
        int eventCount = 1000;
        Arbitrary<TestWalEvent[]> eventsGen = TestWalEventGenerator()
            .Generator
            .ArrayOf(eventCount)
            .ToArbitrary();

        return Prop.ForAll(eventsGen, events =>
        {
            // Arrange
            string directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(directory);
            string filePath = Path.Combine(directory, Path.GetRandomFileName());

            try
            {
                IFilePathCreator pathCreator = new FixedFilePathCreator(filePath);
                ICrcProvider crcProvider = new CrcProvider();

                TestWalReader reader = new(crcProvider);

                // Ack
                using (TestFileAppender appender = new(crcProvider, pathCreator, int.MaxValue))
                {
                    foreach (TestWalEvent testWalEvent in events)
                    {
                        appender.Append(testWalEvent);
                    }
                }
                
                List<TestWalEvent> restoredEvents = reader.Read(filePath).ToList();
                
                // Assert
                bool countMatch = events.Length == restoredEvents.Count;
                
                bool contentMatch = events
                    .Zip(restoredEvents, (orig, restored) => orig.MessageId == restored.MessageId)
                    .All(match => match);
                
                return countMatch && contentMatch;
            }
            finally
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, true);
                }
            }
        });
    }

    [FsCheck.Xunit.Property]
    public Property AbstractWal_ThrowsException_WhenDataIsCorrupted()
    {
        int eventCount = 1000;
        Arbitrary<TestWalEvent[]> eventsGen = TestWalEventGenerator()
            .Generator
            .ArrayOf(eventCount)
            .Where(arr => arr.Length > 0)
            .ToArbitrary();
        
        Arbitrary<double> corruptionFactorGen = Gen.Choose(0, 1000).Select(x => x / 1000.0).ToArbitrary();
        
        return Prop.ForAll(eventsGen, corruptionFactorGen, (events, corruptionFactor) =>
        {
            // Arrange
            string directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(directory);
            string filePath = Path.Combine(directory, Path.GetRandomFileName());

            try
            {
                IFilePathCreator pathCreator = new FixedFilePathCreator(filePath);
                ICrcProvider crcProvider = new CrcProvider();

                TestWalReader reader = new(crcProvider);
                
                using (TestFileAppender appender = new(crcProvider, pathCreator, int.MaxValue))
                {
                    foreach (TestWalEvent testWalEvent in events)
                    {
                        appender.Append(testWalEvent);
                    }
                }
                
                SabotageFile(filePath, corruptionFactor);
                
                // Act + Assert
                try
                {
                    _ = reader.Read(filePath).ToList();
                    return false.ToProperty();
                }
                catch (DataCorruptedException)
                {
                    return true.ToProperty();
                }
                catch (Exception)
                {
                    return false.Label("Unexpected error occurred");
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, true);
                }
            }
        });
    }
    
    private void SabotageFile(string path, double factor)
    {
        using FileStream fs = new(path, FileMode.Open, FileAccess.ReadWrite);
        
        long position = (long)((fs.Length - 1) * factor);
        
        position = Math.Clamp(position, 0, fs.Length - 1);

        fs.Position = position;
        int originalByte = fs.ReadByte();
        
        fs.Position = position;
        fs.WriteByte((byte)~originalByte);
    }
    
    private Arbitrary<TestWalEvent> TestWalEventGenerator()
    {
        Gen<Guid> idGen = ArbMap.Default.GeneratorFor<Guid>();

        Gen<TestWalEvent> generator = 
            from id in idGen
            select new TestWalEvent(id);

        return generator.ToArbitrary();
    }
}