namespace MessageBroker.Engine.Common.Exceptions;

public class SentMessageNotFoundException(Guid messageId)
    : Exception($"Sent message with id '{messageId}' was not found.");