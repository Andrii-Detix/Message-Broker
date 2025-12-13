using System.Text.Json;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Constants;
using MessageBroker.Persistence.Extensions;
using MessageBroker.Persistence.Manifests.Exceptions;

namespace MessageBroker.Persistence.Manifests;

public class ManifestManager : IManifestManager
{
    private readonly WalOptions _walOptions;
    private readonly string _manifestPath;
    private readonly JsonSerializerOptions _jsonOptions;

    public ManifestManager(WalOptions walOptions)
    {
        _walOptions = walOptions;
        _manifestPath = Path.Combine(walOptions.Directory, walOptions.Manifest.FileName);
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
            throw new ManifestFileCorruptedException();
        }
    }

    public void Save(WalManifest manifest)
    {
        var normalizedManifest = new WalManifest
        {
            Enqueue = NormalizeFileName(manifest.Enqueue),
            Ack = NormalizeFileName(manifest.Ack),
            Dead = NormalizeFileName(manifest.Dead),
            EnqueueMerged = NormalizeFileName(manifest.EnqueueMerged),
            AckMerged = NormalizeFileName(manifest.AckMerged),
            DeadMerged = NormalizeFileName(manifest.DeadMerged)
        };
        
        EnsureDirectoryExists();

        string json = JsonSerializer.Serialize(normalizedManifest, _jsonOptions);
        string tempPath = _manifestPath + ".tmp";

        try
        {
            File.WriteAllText(tempPath, json);
            File.Move(tempPath, _manifestPath, overwrite: true);
        }
        catch
        {
            DeleteFile(tempPath);
            throw;
        }
    }


    public WalFiles LoadWalFiles()
    {
        WalManifest manifest = Load();
        
        return new()
        {
            EnqueueFiles = GetAllFiles(_walOptions.FileNaming.EnqueuePrefix, manifest.Enqueue, manifest.EnqueueMerged),
            AckFiles = GetAllFiles(_walOptions.FileNaming.AckPrefix, manifest.Ack, manifest.AckMerged),
            DeadFiles = GetAllFiles(_walOptions.FileNaming.DeadPrefix, manifest.Dead, manifest.DeadMerged)
        };
    }

    private List<string> GetFilesStartingFrom(string prefix, string checkpointFileName)
    {
        if (!Directory.Exists(_walOptions.Directory))
        {
            return [];
        }

        string extension = _walOptions.FileNaming.Extension.TrimStart(FileConstants.ExtensionSeparator);
        string pattern = $"{prefix}{FileConstants.NamePartSeparator}*{FileConstants.NamePartSeparator}*{FileConstants.ExtensionSeparator}{extension}";

        List<string> files = Directory
            .GetFiles(_walOptions.Directory, pattern)
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
            ? Path.Combine(_walOptions.Directory, merged) 
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
    
    private void EnsureDirectoryExists()
    {
        if (!Directory.Exists(_walOptions.Directory))
        {
            Directory.CreateDirectory(_walOptions.Directory);
        }
    }

    private void DeleteFile(string path)
    {
        if (File.Exists(path))
        {
            try
            {
                File.Delete(path);
            }
            catch (Exception)
            {
                // Ignore
            }
        }
    }
}