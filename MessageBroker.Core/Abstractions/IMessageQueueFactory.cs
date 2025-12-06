namespace MessageBroker.Core.Abstractions;

public interface IMessageQueueFactory
{
    IMessageQueue Create();
}