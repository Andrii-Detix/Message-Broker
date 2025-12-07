namespace MessageBroker.Engine.Abstractions;

public interface ICriticalErrorService
{
    void Raise(string message, Exception exception);
}