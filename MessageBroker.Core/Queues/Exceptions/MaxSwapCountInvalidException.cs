namespace MessageBroker.Core.Queues.Exceptions;

public class MaxSwapCountInvalidException()
    : Exception("Max swap count must be equal or greater than one.");