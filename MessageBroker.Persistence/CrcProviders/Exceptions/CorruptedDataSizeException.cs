namespace MessageBroker.Persistence.CrcProviders.Exceptions;

public class CorruptedDataSizeException(int totalSize, int headerSize)
    : Exception($"Corrupted header. Total size {totalSize} is less than header size {headerSize}.");