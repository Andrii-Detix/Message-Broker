namespace MessageBroker.Persistence.Services.GarbageCollector.Exceptions;

public class CollectIntervalInvalidException()
    : Exception("Collect interval must be greater than zero.");