using System.Text.Json;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Extensions;
using Microsoft.Extensions.Options;

namespace MessageBroker.Persistence.Manifests;

public class ManifestManager : IManifestManager
{
    private readonly WalOptions _config;
    private readonly string _manifestPath;
    private readonly JsonSerializerOptions _jsonOptions;

    public ManifestManager(IOptions<WalOptions> options)
    {
        WalOptions config = options.Value;
        
        _config = config;
        _manifestPath = Path.Combine(config.Directory, config.Manifest.FileName);
        _jsonOptions = new JsonSerializerOptions { WriteIndented = true };
    }
    
    public WalManifest Load()
    {
        if (!File.Exists(_manifestPath))
        {
            return new();
        }
        
        try
        {
            string json = File.ReadAllText(_manifestPath);
            return JsonSerializer.Deserialize<WalManifest>(json) ?? new();
        }
        catch
        {
            return new();
        }
    }

    public void Save(WalManifest manifest)
    {
        if (!Directory.Exists(_config.Directory))
        {
            Directory.CreateDirectory(_config.Directory);
        }

        manifest = new()
        {
            Enqueue = NormalizeFileName(manifest.Enqueue),
            Ack = NormalizeFileName(manifest.Ack),
            Dead = NormalizeFileName(manifest.Dead),
            EnqueueMerged = NormalizeFileName(manifest.EnqueueMerged),
            AckMerged = NormalizeFileName(manifest.AckMerged),
            DeadMerged = NormalizeFileName(manifest.DeadMerged)
        };

        string json = JsonSerializer.Serialize(manifest, _jsonOptions);
        File.WriteAllText(_manifestPath, json);
    }

    public WalFiles LoadWalFiles()
    {
        WalManifest manifest = Load();
        
        return new()
        {
            EnqueueFiles = GetAllFiles(_config.FileNaming.EnqueuePrefix, manifest.Enqueue, manifest.EnqueueMerged),
            AckFiles = GetAllFiles(_config.FileNaming.AckPrefix, manifest.Ack, manifest.AckMerged),
            DeadFiles = GetAllFiles(_config.FileNaming.DeadPrefix, manifest.Dead, manifest.DeadMerged)
        };
    }

    private List<string> GetFilesStartingFrom(string prefix, string checkpointFileName)
    {
        if (!Directory.Exists(_config.Directory))
        {
            return [];
        }

        string extension = _config.FileNaming.Extension.TrimStart('.');
        string pattern = $"{prefix}-*-*.{extension}";

        List<string> files = Directory
            .GetFiles(_config.Directory, pattern)
            .OrderByWalFormat()
            .ToList();

        if (!string.IsNullOrWhiteSpace(checkpointFileName))
        {
            string checkpointName = Path.GetFileName(checkpointFileName);

            List<string> filtered = files
                .SkipWhile(f => !Path.GetFileName(f).Equals(checkpointName, StringComparison.OrdinalIgnoreCase))
                .ToList();

            bool invalidCheckpoint = files.Count > 0 && filtered.Count == 0;

            if (!invalidCheckpoint)
            {
                files = filtered;
            }
        }

        return files;
    }

    private List<string> GetAllFiles(string prefix, string checkpoint, string merged)
    {
        List<string> files = GetFilesStartingFrom(prefix, checkpoint);
        
        string mergedPath = !string.IsNullOrWhiteSpace(merged) 
            ? Path.Combine(_config.Directory, merged) 
            : string.Empty;
        
        if (!string.IsNullOrEmpty(mergedPath))
        {
            files.RemoveAll(f => f.Equals(mergedPath, StringComparison.OrdinalIgnoreCase));
        }
        
        if (!string.IsNullOrEmpty(mergedPath) && File.Exists(mergedPath))
        {
            files.Insert(0, mergedPath);
        }
        
        return files;
    }

    private string NormalizeFileName(string fileName)
    {
        return string.IsNullOrWhiteSpace(fileName)
            ? string.Empty
            : Path.GetFileName(fileName);
    }
}