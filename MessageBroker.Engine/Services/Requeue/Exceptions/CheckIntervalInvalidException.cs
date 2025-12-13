namespace MessageBroker.Engine.Services.Requeue.Exceptions;

public class CheckIntervalInvalidException()
    : Exception("Check interval must be greater than zero.");