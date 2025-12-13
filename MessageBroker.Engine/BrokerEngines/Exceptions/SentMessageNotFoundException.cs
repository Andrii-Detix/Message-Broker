namespace MessageBroker.Engine.BrokerEngines.Exceptions;

public class SentMessageNotFoundException(Guid messageId)
    : Exception($"Sent message with id '{messageId}' was not found.");