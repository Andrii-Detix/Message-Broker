namespace MessageBroker.Core.ExpiredMessagePolicies.Exceptions;

public class MessageExpirationTimeInvalidException()
    : Exception("Message expiration time must be greater than zero.");