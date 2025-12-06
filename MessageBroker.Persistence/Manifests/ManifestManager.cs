using System.Text.Json;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Extensions;
using Microsoft.Extensions.Options;

namespace MessageBroker.Persistence.Manifests;

public class ManifestManager : IManifestManager
{
    private readonly WalConfiguration _config;
    private readonly string _manifestPath;
    private readonly JsonSerializerOptions _jsonOptions;

    public ManifestManager(IOptions<WalConfiguration> options)
    {
        WalConfiguration config = options.Value;
        
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
            Merged = NormalizeFileName(manifest.Merged)
        };

        string json = JsonSerializer.Serialize(manifest, _jsonOptions);
        File.WriteAllText(_manifestPath, json);
    }

    public WalFiles LoadWalFiles()
    {
        WalManifest manifest = Load();

        string mergedPath = !string.IsNullOrEmpty(manifest.Merged) 
            ? Path.Combine(_config.Directory, manifest.Merged) 
            : string.Empty;
        
        if (!string.IsNullOrEmpty(mergedPath) && !File.Exists(mergedPath))
        {
            mergedPath = string.Empty;
        }
        
        return new()
        {
            EnqueueFiles = GetFilesStartingFrom(_config.FileBaseNames.Enqueue, manifest.Enqueue),
            AckFiles = GetFilesStartingFrom(_config.FileBaseNames.Ack, manifest.Ack),
            DeadFiles = GetFilesStartingFrom(_config.FileBaseNames.Dead, manifest.Dead),
            MergedFile = mergedPath
        };
    }

    private List<string> GetFilesStartingFrom(string prefix, string checkpointFileName)
    {
        if (!Directory.Exists(_config.Directory))
        {
            return [];
        }

        string extension = _config.FileExtension.TrimStart('.');
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

    private string NormalizeFileName(string fileName)
    {
        return string.IsNullOrWhiteSpace(fileName)
            ? string.Empty
            : Path.GetFileName(fileName);
    }
}