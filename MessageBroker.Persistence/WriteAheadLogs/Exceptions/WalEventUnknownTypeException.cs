namespace MessageBroker.Persistence.WriteAheadLogs.Exceptions;

public class WalEventUnknownTypeException()
    : Exception("Unknown type of wal event.");