namespace MessageBroker.Core.Messages.Exceptions;

public class InvalidRestoreMessageStateException()
    : Exception("Restored message cannot be only in restored, delivered or failed state.");