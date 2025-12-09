using System.Text.Json;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Manifests;
using Microsoft.Extensions.Options;
using Shouldly;

namespace MessageBroker.IntegrationTests.Persistence.Manifests;

public class ManifestManagerTests : IDisposable
{
    private readonly string _directory;
    private readonly WalOptions _config;

    public ManifestManagerTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(_directory);

        _config = new WalOptions
        {
            Directory = _directory,
            Manifest = new ManifestOptions
            {
                FileName = "manifest.json"
            },
            FileNaming = new FileNamingOptions 
            { 
                Extension = "log",
                EnqueuePrefix = "enqueue", 
                AckPrefix = "ack", 
                DeadPrefix = "dead",
                EnqueueMergedPrefix = "enqueue-merged",
                AckMergedPrefix = "ack-merged",
                DeadMergedPrefix = "dead-merged"
            }
        };
    }
    
    [Fact]
    public void Load_ReturnsEmptyManifest_WhenFileDoesNotExist()
    {
        // Arrange
        ManifestManager sut = new(Options.Create(_config));

        // Act
        WalManifest actual = sut.Load();

        // Assert
        actual.ShouldNotBeNull();
        actual.Enqueue.ShouldBeEmpty();
        actual.Ack.ShouldBeEmpty();
        actual.Dead.ShouldBeEmpty();
        actual.EnqueueMerged.ShouldBeEmpty();
        actual.AckMerged.ShouldBeEmpty();
    }
    
    [Fact]
    public void Load_ReturnsEmptyManifest_WhenFileIsCorrupted()
    {
        // Arrange
        string manifestPath = Path.Combine(_directory, "manifest.json");
        File.WriteAllText(manifestPath, "{ invalid json ... ");
        ManifestManager sut = new(Options.Create(_config));

        // Act
        WalManifest actual = sut.Load();

        // Assert
        actual.ShouldNotBeNull();
        actual.Enqueue.ShouldBeEmpty();
        actual.Ack.ShouldBeEmpty();
        actual.Dead.ShouldBeEmpty();
        actual.EnqueueMerged.ShouldBeEmpty();
        actual.EnqueueMerged.ShouldBeEmpty();
        actual.AckMerged.ShouldBeEmpty();
    }

    [Fact]
    public void Load_ReturnsWalManifest_WhenFileExists()
    {
        // Arrange
        string manifestPath = Path.Combine(_directory, "manifest.json");
        ManifestManager sut = new(Options.Create(_config));

        WalManifest manifest = new()
        {
            Enqueue = "enqueue-20251205180044-1.log",
            Ack = "ack-20251205180044-1.log",
            Dead = "dead-20251205180044-1.log",
            EnqueueMerged = "enqueue-merged-20251205180044-1.log",
            AckMerged = "ack-merged-20251205180044-1.log",
            DeadMerged = "dead-merged-20251205180044-1.log"
        };
        
        string json = JsonSerializer.Serialize(manifest, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(manifestPath, json);
        
        // Act
        WalManifest actual = sut.Load();
        
        // Assert
        actual.ShouldNotBeNull();
        actual.ShouldBe(manifest);
    }

    [Fact]
    public void Save_WritesManifestToFile()
    {
        // Arrange
        ManifestManager sut = new(Options.Create(_config));
        WalManifest manifest = new()
        {
            Enqueue = "enqueue-20251205180044-1.log",
            Ack = "ack-20251205180044-1.log",
            Dead = "dead-20251205180044-1.log",
            EnqueueMerged = "enqueue-merged-20251205180044-1.log",
            AckMerged = "ack-merged-20251205180044-1.log",
            DeadMerged = "dead-merged-20251205180044-1.log"
        };
        
        // Act
        sut.Save(manifest);
        
        // Assert
        File.Exists(Path.Combine(_directory, "manifest.json")).ShouldBeTrue();
        
        WalManifest actual = sut.Load();
        actual.ShouldNotBeNull();
        actual.ShouldBe(manifest);
    }

    [Fact]
    public void Save_NormalizeFileNames_WhenContainsRedundantParts()
    {
        // Arrange
        ManifestManager sut = new(Options.Create(_config));
        WalManifest manifest = new()
        {
            Enqueue = "some/directory/enqueue-20251205180044-1.log",
            Ack = "    ",
            Dead = null!,
            EnqueueMerged = "enqueue-merged--20251205180044-1.log"
        };
        
        // Act
        sut.Save(manifest);
        
        // Assert
        WalManifest actual = sut.Load();
        
        actual.Enqueue.ShouldBe("enqueue-20251205180044-1.log");
        actual.Ack.ShouldBe(string.Empty);
        actual.Dead.ShouldBe(string.Empty);
        actual.EnqueueMerged.ShouldBe("enqueue-merged--20251205180044-1.log");
    }

    [Fact]
    public void LoadWalFiles_FiltersFiles_BasedOnCheckpoint()
    {
        // Arrange
        string file1 = "enqueue-20251205180044-1.log";
        string file2 = "enqueue-20251205180044-2.log";
        string file3 = "enqueue-20251205180045-1.log";

        File.Create(Path.Combine(_directory, file1)).Dispose();
        File.Create(Path.Combine(_directory, file2)).Dispose();
        File.Create(Path.Combine(_directory, file3)).Dispose();

        ManifestManager sut = new(Options.Create(_config));
        sut.Save(new() {Enqueue = file2});
        
        // Act
        WalFiles actual = sut.LoadWalFiles();
        
        // Assert
        actual.ShouldNotBeNull();
        List<string?> enqueues = actual.EnqueueFiles.Select(Path.GetFileName).ToList();
        enqueues.Count.ShouldBe(2);
        
        enqueues.ShouldContain(file2);
        enqueues.ShouldContain(file3);
        enqueues.ShouldNotContain(file1);
    }
    
    [Fact]
    public void LoadWalFiles_ReturnsAllFiles_WhenCheckpointIsMissing()
    {
        // Arrange
        string file1 = "enqueue-20251205180044-1.log";
        string file2 = "enqueue-20251205180044-2.log";
        string file3 = "enqueue-20251205180045-1.log";

        File.Create(Path.Combine(_directory, file1)).Dispose();
        File.Create(Path.Combine(_directory, file2)).Dispose();
        File.Create(Path.Combine(_directory, file3)).Dispose();

        ManifestManager sut = new(Options.Create(_config));
        sut.Save(new() {Enqueue = "enqueue-20251205180045-100000.log"});
        
        // Act
        WalFiles actual = sut.LoadWalFiles();
        
        // Assert
        actual.ShouldNotBeNull();
        List<string?> enqueues = actual.EnqueueFiles.Select(Path.GetFileName).ToList();
        enqueues.Count.ShouldBe(3);
        
        enqueues.ShouldContain(file1);
        enqueues.ShouldContain(file2);
        enqueues.ShouldContain(file3);
    }

    [Fact]
    public void LoadWalFiles_SortsFilesChronologically()
    {
        // Arrange
        string file1 = "enqueue-20251205180044-2.log";
        string file2 = "enqueue-20251205180044-1.log";
        string file3 = "enqueue-20251205180043-1.log";
        string file4 = "enqueue-20251205180045-2.log";

        File.Create(Path.Combine(_directory, file1)).Dispose();
        File.Create(Path.Combine(_directory, file2)).Dispose();
        File.Create(Path.Combine(_directory, file3)).Dispose();
        File.Create(Path.Combine(_directory, file4)).Dispose();

        ManifestManager sut = new(Options.Create(_config));
        
        // Act
        WalFiles actual = sut.LoadWalFiles();
        
        // Assert
        actual.ShouldNotBeNull();
        List<string?> enqueues = actual.EnqueueFiles.Select(Path.GetFileName).ToList();
        enqueues.Count.ShouldBe(4);
        
        enqueues[0].ShouldBe(file3);
        enqueues[1].ShouldBe(file2);
        enqueues[2].ShouldBe(file1);
        enqueues[3].ShouldBe(file4);
    }

    [Fact]
    public void LoadWalFiles_ReturnsWithMergedFile_WhenItExistsOnDisk()
    {
        // Arrange
        ManifestManager sut = new(Options.Create(_config));
        string mergedFile = "enqueue-merged-20251205180045-2.log";
        string file1 = "enqueue-20251205180044-1.log";
        string file2 = "enqueue-20251205180044-2.log";
        string file3 = "enqueue-20251205180045-1.log";
        
        File.Create(Path.Combine(_directory, file1)).Dispose();
        File.Create(Path.Combine(_directory, file2)).Dispose();
        File.Create(Path.Combine(_directory, file3)).Dispose();
        File.Create(Path.Combine(_directory, mergedFile)).Dispose();
        
        sut.Save(new WalManifest { EnqueueMerged = mergedFile });
        
        // Act
        WalFiles actual = sut.LoadWalFiles();
        
        // Assert
        actual.ShouldNotBeNull();
        List<string?> enqueues = actual.EnqueueFiles.Select(Path.GetFileName).ToList();
        enqueues.Count.ShouldBe(4);
        
        enqueues[0].ShouldBe(mergedFile);
        enqueues[1].ShouldBe(file1);
        enqueues[2].ShouldBe(file2);
        enqueues[3].ShouldBe(file3);
    }
    
    [Fact]
    public void LoadWalFiles_LoadsAllFileTypes_Simultaneously()
    {
        // Arrange
        string enqueue = "enqueue-20251205180045-2.log";
        string ack = "ack-20250101-20251205180045-2.log";
        string dead = "dead-20250101-20251205180045-2.log";
        string enqueueMerged = "enqueue-merged-20251205180045-2.log";
        string ackMerged = "ack-merged-20251205180045-2.log";
        string deadMerged = "dead-merged-20251205180045-2.log";

        File.Create(Path.Combine(_directory, enqueue)).Dispose();
        File.Create(Path.Combine(_directory, ack)).Dispose();
        File.Create(Path.Combine(_directory, dead)).Dispose();
        File.Create(Path.Combine(_directory, enqueueMerged)).Dispose();
        File.Create(Path.Combine(_directory, ackMerged)).Dispose();
        File.Create(Path.Combine(_directory, deadMerged)).Dispose();

        ManifestManager sut = new(Options.Create(_config));
    
        // Act
        WalFiles actual = sut.LoadWalFiles();

        // Assert
        actual.EnqueueFiles.ShouldContain(f => f.EndsWith(enqueue));
        actual.EnqueueFiles.ShouldContain(f => f.EndsWith(enqueueMerged));
        actual.AckFiles.ShouldContain(f => f.EndsWith(ack));
        actual.AckFiles.ShouldContain(f => f.EndsWith(ackMerged));
        actual.DeadFiles.ShouldContain(f => f.EndsWith(dead));
        actual.DeadFiles.ShouldContain(f => f.EndsWith(deadMerged));
    }
    
    [Fact]
    public void LoadWalFiles_ReturnsEmptyLists_WhenWalDirectoryDoesNotExist()
    {
        // Arrange
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    
        ManifestManager sut = new(Options.Create(_config));

        // Act
        WalFiles actual = sut.LoadWalFiles();

        // Assert
        actual.ShouldNotBeNull();
        actual.EnqueueFiles.ShouldBeEmpty();
        actual.AckFiles.ShouldBeEmpty();
        actual.DeadFiles.ShouldBeEmpty();
    }
    
    [Fact]
    public void LoadWalFiles_ReturnsAllFiles_WhenCheckpointIsExplicitlyEmpty()
    {
        // Arrange
        string file1 = "enqueue-20251205180045-1.log";
        string file2 = "enqueue-20251205180045-2.log";
        File.Create(Path.Combine(_directory, file1)).Dispose();
        File.Create(Path.Combine(_directory, file2)).Dispose();

        ManifestManager sut = new(Options.Create(_config));
    
        sut.Save(new WalManifest { Enqueue = string.Empty }); 

        // Act
        WalFiles actual = sut.LoadWalFiles();

        // Assert
        actual.EnqueueFiles.Count.ShouldBe(2);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, true);
        }
    }
}