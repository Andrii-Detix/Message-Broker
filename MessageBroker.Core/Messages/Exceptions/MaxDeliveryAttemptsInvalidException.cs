namespace MessageBroker.Core.Messages.Exceptions;

public class MaxDeliveryAttemptsInvalidException()
    : Exception("Max delivery attempts must be equal or greater than one.");