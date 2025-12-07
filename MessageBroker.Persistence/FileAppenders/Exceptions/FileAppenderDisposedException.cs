using MessageBroker.Persistence.Common.Exceptions;

namespace MessageBroker.Persistence.FileAppenders.Exceptions;

public class FileAppenderDisposedException()
    : WalStorageException("File appender is disposed.");