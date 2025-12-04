namespace MessageBroker.Engine.RequeueServices.Exceptions;

public class CheckIntervalInvalidException()
    : Exception("Check interval must be greater than zero.");