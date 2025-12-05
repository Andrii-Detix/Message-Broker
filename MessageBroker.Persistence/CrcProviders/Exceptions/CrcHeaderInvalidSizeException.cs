namespace MessageBroker.Persistence.CrcProviders.Exceptions;

public class CrcHeaderInvalidSizeException(int headerSize)
    : Exception($"Crc header must contain {headerSize} bytes.");