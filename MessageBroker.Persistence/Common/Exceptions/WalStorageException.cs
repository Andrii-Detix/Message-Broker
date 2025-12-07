namespace MessageBroker.Persistence.Common.Exceptions;

public class WalStorageException(string message, Exception? innerException = null)
    : Exception(message, innerException);