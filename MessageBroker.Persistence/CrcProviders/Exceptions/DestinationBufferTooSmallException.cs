namespace MessageBroker.Persistence.CrcProviders.Exceptions;

public class DestinationBufferTooSmallException(int headerSize) 
    : Exception($"Destination buffer size must be at least {headerSize} bytes.");