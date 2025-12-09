using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.Manifests;
namespace MessageBroker.Persistence.Recovery;

public class RecoveryService : IRecoveryService
{
    private readonly IManifestManager _manifestManager;
    private readonly IWalReader<EnqueueWalEvent> _enqueueWalReader;
    private readonly IWalReader<AckWalEvent> _ackWalReader;
    private readonly IWalReader<DeadWalEvent> _deadWalReader;
    private readonly IMessageQueueFactory _queueFactory;
    private readonly WalOptions _walOptions;
    private readonly MessageOptions _messageOptions;
    private readonly TimeProvider _timeProvider;

    public RecoveryService(
        IManifestManager manifestManager,
        IWalReader<EnqueueWalEvent> enqueueWalReader,
        IWalReader<AckWalEvent> ackWalReader,
        IWalReader<DeadWalEvent> deadWalReader,
        IMessageQueueFactory queueFactory,
        WalOptions walOptions,
        MessageOptions messageOptions,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(manifestManager);
        ArgumentNullException.ThrowIfNull(enqueueWalReader);
        ArgumentNullException.ThrowIfNull(ackWalReader);
        ArgumentNullException.ThrowIfNull(deadWalReader);
        ArgumentNullException.ThrowIfNull(queueFactory);
        ArgumentNullException.ThrowIfNull(walOptions);
        ArgumentNullException.ThrowIfNull(messageOptions);
        ArgumentNullException.ThrowIfNull(timeProvider);
        
        _manifestManager = manifestManager;
        _enqueueWalReader = enqueueWalReader;
        _ackWalReader = ackWalReader;
        _deadWalReader = deadWalReader;
        _queueFactory = queueFactory;
        _walOptions = walOptions;
        _messageOptions = messageOptions;
        _timeProvider = timeProvider;
    }
    
    public IMessageQueue Recover()
    {
        if (_walOptions.ResetOnStartup)
        {
            ResetData();
            return _queueFactory.Create();
        }
        
        IMessageQueue queue = RecoverQueue();
        
        return queue;
    }

    private void ResetData()
    {
        string directory = _walOptions.Directory;
        
        if (!Directory.Exists(directory))
        {
            return;
        }
        
        Directory.Delete(directory, true);
        
        Directory.CreateDirectory(directory);
    }
    
    private IMessageQueue RecoverQueue()
    {
        IMessageQueue queue = _queueFactory.Create();
        
        LinkedList<RecoveredMessageDto> pendingMessages = ReconstructRecoveryMessages();
        
        foreach (var pendingMessageDto in pendingMessages)
        {
            Message message = MapToDomainMessage(pendingMessageDto);
            
            queue.TryEnqueue(message);
        }
        
        return queue;
    }

    private LinkedList<RecoveredMessageDto> ReconstructRecoveryMessages()
    {
        WalFiles files = _manifestManager.LoadWalFiles();
        
        HashSet<Guid> ignoredIds = LoadIgnoredMessageIds(files);

        IEnumerable<EnqueueWalEvent> enqueueWalEvents = ReadEvents(_enqueueWalReader, files.EnqueueFiles);

        LinkedList<RecoveredMessageDto> orderedMessages = [];
        Dictionary<Guid, LinkedListNode<RecoveredMessageDto>> messageNodes = [];
        
        foreach (var enqueueWalEvent in enqueueWalEvents)
        {
            Guid messageId = enqueueWalEvent.MessageId;
            
            if (enqueueWalEvent is RequeueWalEvent)
            {
                if (messageNodes.TryGetValue(messageId, out var existingNode))
                {
                    MoveToBack(orderedMessages, existingNode);
                }
                
                continue;
            }
            
            if (ignoredIds.Contains(messageId))
            {
                continue;
            }

            RecoveredMessageDto dto = new(enqueueWalEvent.MessageId, enqueueWalEvent.Payload);
            LinkedListNode<RecoveredMessageDto> newNode = orderedMessages.AddLast(dto);
            
            messageNodes.TryAdd(messageId, newNode);
        }

        return orderedMessages;
    }
    
    private HashSet<Guid> LoadIgnoredMessageIds(WalFiles files)
    {
        IEnumerable<Guid> ackIds = ReadEvents(_ackWalReader, files.AckFiles)
            .Select(e => e.MessageId);
        
        IEnumerable<Guid> deadIds = ReadEvents(_deadWalReader, files.DeadFiles)
            .Select(e => e.MessageId);

        return ackIds.Concat(deadIds).ToHashSet();
    }
    
    private IEnumerable<TEvent> ReadEvents<TEvent>(IWalReader<TEvent> walReader, List<string> files)
        where TEvent : WalEvent
    {
        return files.SelectMany(walReader.Read);
    }

    private static void MoveToBack(
        LinkedList<RecoveredMessageDto> list,
        LinkedListNode<RecoveredMessageDto> node)
    {
        node.Value.DeliveryAttempts++;
        list.Remove(node);
        list.AddLast(node);
    }

    private Message MapToDomainMessage(RecoveredMessageDto dto)
    {
        return Message.Restore(
            dto.MessageId,
            dto.Payload,
            MessageState.Restored,
            _timeProvider.GetUtcNow(),
            null,
            dto.DeliveryAttempts,
            _messageOptions.MaxDeliveryAttempts);
    }
}