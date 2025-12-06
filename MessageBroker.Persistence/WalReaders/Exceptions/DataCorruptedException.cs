namespace MessageBroker.Persistence.WalReaders.Exceptions;

public class DataCorruptedException()
    : Exception("Stored data is corrupted.");