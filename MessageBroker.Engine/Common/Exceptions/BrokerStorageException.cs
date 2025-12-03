namespace MessageBroker.Engine.Common.Exceptions;

public class BrokerStorageException()
    : Exception("Failed to persist data to the Write-Ahead Log.");