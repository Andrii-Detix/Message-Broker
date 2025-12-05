namespace MessageBroker.Core.Messages.Exceptions;

public class DeliveryCountInvalidException()
    : Exception("Delivery count cannot be negative.");