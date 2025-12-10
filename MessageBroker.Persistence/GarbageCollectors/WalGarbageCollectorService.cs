using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.Manifests;

namespace MessageBroker.Persistence.GarbageCollectors;

public class WalGarbageCollectorService : IWalGarbageCollectorService
{
    private readonly IManifestManager _manifestManager;
    private readonly IWalReader<EnqueueWalEvent> _enqueueWalReader;
    private readonly IWalReader<AckWalEvent> _ackWalReader;
    private readonly IWalReader<DeadWalEvent> _deadWalReader;
    private readonly IFileAppenderFactory _appenderFactory;

    public WalGarbageCollectorService(
        IManifestManager manifestManager,
        IWalReader<EnqueueWalEvent> enqueueWalReader,
        IWalReader<AckWalEvent> ackWalReader,
        IWalReader<DeadWalEvent> deadWalReader,
        IFileAppenderFactory appenderFactory)
    {
        ArgumentNullException.ThrowIfNull(manifestManager);
        ArgumentNullException.ThrowIfNull(enqueueWalReader);
        ArgumentNullException.ThrowIfNull(ackWalReader);
        ArgumentNullException.ThrowIfNull(deadWalReader);
        ArgumentNullException.ThrowIfNull(appenderFactory);
        
        _manifestManager = manifestManager;
        _enqueueWalReader = enqueueWalReader;
        _ackWalReader = ackWalReader;
        _deadWalReader = deadWalReader;
        _appenderFactory = appenderFactory;
    }
    
    public void Collect()
    {
        WalFiles files = _manifestManager.LoadWalFiles();
        
        string[] enqFiles = files.EnqueueFiles.SkipLast(1).ToArray();
        string[] ackFiles = files.AckFiles.SkipLast(1).ToArray();
        string[] deadFiles = files.DeadFiles.SkipLast(1).ToArray();

        (string enqueueMerged, string ackMerged, string deadMerged) mergedFiles = CollectData(
            enqFiles,
            ackFiles,
            deadFiles);

        WalManifest manifest = new()
        {
            Enqueue = files.EnqueueFiles.LastOrDefault() ?? string.Empty,
            Ack = files.AckFiles.LastOrDefault() ?? string.Empty,
            Dead = files.DeadFiles.LastOrDefault() ?? string.Empty,

            EnqueueMerged = mergedFiles.enqueueMerged,
            AckMerged = mergedFiles.ackMerged,
            DeadMerged = mergedFiles.deadMerged,
        };
        
        _manifestManager.Save(manifest);

        string[] removalFiles = enqFiles
            .Concat(ackFiles)
            .Concat(deadFiles)
            .ToArray();
        
        DeleteFiles(removalFiles);
    }

    private (string enqFile, string ackFile, string deadFile) CollectData(
        string[] enqueueFiles,
        string[] ackFiles,
        string[] deadFiles)
    {
        string enqMerged = string.Empty;
        string ackMerged = string.Empty;
        string deadMerged = string.Empty;
        
        HashSet<Guid> ackIds = ReadEvents(_ackWalReader, ackFiles)
            .Select(e => e.MessageId)
            .ToHashSet();

        HashSet<Guid> deadIds = ReadEvents(_deadWalReader, deadFiles)
            .Select(e => e.MessageId)
            .ToHashSet();

        HashSet<Guid> activeMessages = [];

        IEnumerable<EnqueueWalEvent> enqueueEvents = ReadEvents(_enqueueWalReader, enqueueFiles);

        try
        {
            using var enqueueAppender = _appenderFactory.Create<EnqueueWalEvent>();
            enqMerged = enqueueAppender.CurrentFile;
            
            foreach (var enqueueEvent in enqueueEvents)
            {
                Guid messageId = enqueueEvent.MessageId;

                bool isAck = ackIds.Remove(messageId);
                bool isDead = deadIds.Remove(messageId);

                if (isAck || isDead)
                {
                    continue;
                }

                if (enqueueEvent is RequeueWalEvent)
                {
                    if (activeMessages.Contains(messageId))
                    {
                        enqueueAppender.Append(enqueueEvent);
                    }

                    continue;
                }

                enqueueAppender.Append(enqueueEvent);
                activeMessages.Add(messageId);
            }

            if (ackIds.Count > 0)
            {
                using var ackAppender = _appenderFactory.Create<AckWalEvent>();
                ackMerged = ackAppender.CurrentFile;
                
                foreach (var ackId in ackIds)
                {
                    AckWalEvent ackEvent = new(ackId);
                    ackAppender.Append(ackEvent);
                }
            }

            if (deadIds.Count > 0)
            {
                using var deadAppender = _appenderFactory.Create<DeadWalEvent>();
                deadMerged = deadAppender.CurrentFile;
                
                foreach (var deadId in deadIds)
                {
                    DeadWalEvent deadEvent = new(deadId);
                    deadAppender.Append(deadEvent);
                }
            }

            return (enqMerged, ackMerged, deadMerged);
        }
        catch
        {
            DeleteFiles(enqMerged, ackMerged, deadMerged);
            throw;
        }
    }
    
    private IEnumerable<TEvent> ReadEvents<TEvent>(IWalReader<TEvent> walReader, string[] files)
        where TEvent : WalEvent
    {
        return files.SelectMany(walReader.Read);
    }

    private void DeleteFiles(params string[] files)
    {
        foreach (var file in files)
        {
            if (File.Exists(file))
            {
                File.Delete(file);
            }
        }
    }
}