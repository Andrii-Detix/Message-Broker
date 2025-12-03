namespace MessageBroker.Engine.BrokerEngines.Exceptions;

public class PayloadTooLargeException(int currentSize, int maxSize)
    : Exception($"Payload size of {currentSize} exceeds maximum allowed length of {maxSize}.");