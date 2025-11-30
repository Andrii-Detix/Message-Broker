using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;

namespace MessageBroker.IntegrationTests.Persistence.FileAppenders;

public record TestWalEvent() : WalEvent(WalEventType.Enqueue);

public class TestFileAppender(
    IFilePathCreator? pathCreator,
    int maxWriteCountPerFile)
    : AbstractFileAppender<TestWalEvent>(pathCreator, maxWriteCountPerFile)
{
    public override void Append(TestWalEvent evt) { }

    public void WriteRawBytes(byte[] data)
    {
        SaveData(data);
    }
}