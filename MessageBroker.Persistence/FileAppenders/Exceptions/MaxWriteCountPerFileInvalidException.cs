namespace MessageBroker.Persistence.FileAppenders.Exceptions;

public class MaxWriteCountPerFileInvalidException()
    : Exception("Max write count per file mast be equal or greater than one.");