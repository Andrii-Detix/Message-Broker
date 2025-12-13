using FsCheck;
using FsCheck.Fluent;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Core.Queues;
using Microsoft.Extensions.Time.Testing;

namespace MessageBroker.UnitTests.Core.Queues;

public class MessageQueuePropertyTests
{
    private readonly FakeTimeProvider _timeProvider;

    public MessageQueuePropertyTests()
    {
        _timeProvider = new();
    }
    
    [FsCheck.Xunit.Property]
    public Property MessageQueue_MaintainsFifoOrder_WhenPublishesAndConsumesMessages()
    {
        int messageCount = 5000;
        Arbitrary<Message[]> messageArrGen = MessageGenerator().Generator.ArrayOf(messageCount).ToArbitrary();
        Arbitrary<int> maxSwapCountGen = Gen.Choose(1, messageCount).ToArbitrary();

        return Prop.ForAll(messageArrGen, maxSwapCountGen, (inputMessages, maxSwapCount) =>
        {
            // Arrange
            Message[] messages = inputMessages.DistinctBy(m => m.Id).ToArray();
        
            MessageQueue sut = new(maxSwapCount, _timeProvider);

            foreach (Message message in messages)
            {
                if (!sut.TryEnqueue(message))
                {
                    return false;
                }
            }

            List<Message> consumedMessages = [];
            
            // Act
            while (sut.TryConsume(out Message? msg))
            {
                consumedMessages.Add(msg!);
            }

            // Assert
            bool countMatch = messages.Length == consumedMessages.Count;
        
            bool orderMatch = messages
                .Select(m => m.Id)
                .SequenceEqual(consumedMessages.Select(m => m.Id));

            return countMatch && orderMatch;
        });
    }

    [FsCheck.Xunit.Property]
    public Property MessageQueue_MaintainsFifoOrder_WithIntervaledOperations()
    {
        int messageCount = 5000;
        Arbitrary<Message[]> messageArrGen = MessageGenerator().Generator.ArrayOf(messageCount).ToArbitrary();
        Arbitrary<int[]> stepsGen = Gen.Choose(-10, 10).ArrayOf().ToArbitrary();
        Arbitrary<int> maxSwapCountGen = Gen.Choose(1, messageCount).ToArbitrary();

        return Prop.ForAll(
            messageArrGen, 
            stepsGen, 
            maxSwapCountGen, 
            (inputMessages, steps, maxSwapCount) =>
        {
            // Arrange
            Message[] messages = inputMessages.DistinctBy(m => m.Id).ToArray();
            if (messages.Length == 0)
            {
                return true;
            }
            
            int producerIndex = 0;
            List<Message> consumedMessages = [];
            
            MessageQueue sut = new(maxSwapCount, _timeProvider);

            // Act
            foreach (int step in steps)
            {
                if (step > 0)
                {
                    int addCount = 0;
                    
                    while (addCount < step && producerIndex < messages.Length)
                    {
                        Message message = messages[producerIndex];
                        bool enqueued = sut.TryEnqueue(message);
                        
                        if (!enqueued)
                        {
                            return false;
                        } 

                        producerIndex++;
                        addCount++;
                    }
                }
                else if (step < 0)
                {
                    int consumeCount = 0;

                    while (consumeCount > step)
                    {
                        if (sut.TryConsume(out Message? message))
                        {
                            consumedMessages.Add(message!);
                        }
                        
                        consumeCount--;
                    }
                }
            }

            while (sut.TryConsume(out Message? message))
            {
                consumedMessages.Add(message!);
            }
            
            // Assert
            bool countMatch = consumedMessages.Count == producerIndex;

            bool orderMatch = messages.Take(consumedMessages.Count)
                .Select(m => m.Id)
                .SequenceEqual(consumedMessages.Select(m => m.Id));
            
            return countMatch && orderMatch;
        });
    }
    
    private Arbitrary<Message> MessageGenerator()
    {
        Gen<Guid> idGen = ArbMap.Default.GeneratorFor<Guid>();
        Gen<byte[]> contentGen = ArbMap.Default.GeneratorFor<byte[]>();
        Gen<int> attemptsGen = Gen.Choose(1, 1000);

        Gen<Message> generator = 
            from id in idGen
            from content in contentGen
            from attempts in attemptsGen
            select CreateMessageSafe(id, content, attempts);

        return generator.ToArbitrary();
    }

    private Message CreateMessageSafe(Guid id, byte[]? content, int attempts)
    {
        content ??= [];
        return Message.Create(id, content, attempts, _timeProvider);
    }
}