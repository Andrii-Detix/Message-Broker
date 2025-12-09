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

    private IEnumerable<TEvent> GetEvents<TEvent>(IWalReader<TEvent> walReader, List<string> files)
        where TEvent : WalEvent
    {
        return files.SelectMany(walReader.Read);
    }

    private LinkedList<RecoveredMessageDto> RestoreRecoveryMessageDtos()
    {
        WalFiles walFiles = _manifestManager.LoadWalFiles();
        
        IEnumerable<Guid> ackIds = GetEvents(_ackWalReader, walFiles.AckFiles)
            .Select(e => e.MessageId);
        
        IEnumerable<Guid> deadIds = GetEvents(_deadWalReader, walFiles.DeadFiles)
            .Select(e => e.MessageId);
        
        HashSet<Guid> completedIds = ackIds.Concat(deadIds).ToHashSet();

        IEnumerable<EnqueueWalEvent> enqueueWalEvents = GetEvents(_enqueueWalReader, walFiles.EnqueueFiles);

        LinkedList<RecoveredMessageDto> orderedMessages = [];
        Dictionary<Guid, LinkedListNode<RecoveredMessageDto>> nodeLookup = [];
        
        foreach (var enqueueWalEvent in enqueueWalEvents)
        {
            Guid messageId = enqueueWalEvent.MessageId;

            if (completedIds.Contains(messageId))
            {
                continue;
            }

            if (nodeLookup.TryGetValue(messageId, out LinkedListNode<RecoveredMessageDto>? node))
            {
                node.Value.DeliveryAttempts++;
                
                orderedMessages.Remove(node);
                orderedMessages.AddLast(node);
                
                continue;
            }

            RecoveredMessageDto dto = new(enqueueWalEvent.MessageId, enqueueWalEvent.Payload);
            
            node = orderedMessages.AddLast(dto);
            nodeLookup.TryAdd(messageId, node);
        }

        return orderedMessages;
    }

    private Message RestoreMessage(RecoveredMessageDto dto)
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

    private IMessageQueue RecoverQueue()
    {
        IMessageQueue queue = _queueFactory.Create();
        
        LinkedList<RecoveredMessageDto> dtos = RestoreRecoveryMessageDtos();
        
        foreach (var dto in dtos)
        {
            Message message = RestoreMessage(dto);
            
            queue.TryEnqueue(message);
        }
        
        return queue;
    }
}