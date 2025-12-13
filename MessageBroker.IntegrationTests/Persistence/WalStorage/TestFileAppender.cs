using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.FileAppenders;

namespace MessageBroker.IntegrationTests.Persistence.WalStorage;

public class TestFileAppender(
    ICrcProvider? crcProvider, 
    IFilePathCreator? filePathCreator, 
    int maxWriteCountPerFile)  
    : AbstractFileAppender<TestWalEvent>(crcProvider, filePathCreator, maxWriteCountPerFile)
{
    public override void Append(TestWalEvent evt)
    {
        byte[] buffer = evt.MessageId.ToByteArray();
        
        SaveData(buffer);
    }
}