namespace MessageBroker.Persistence.GarbageCollectors.Exceptions;

public class CollectIntervalInvalidException()
    : Exception("Collect interval must be greater than zero.");