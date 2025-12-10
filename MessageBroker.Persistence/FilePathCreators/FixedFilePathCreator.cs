using MessageBroker.Persistence.Abstractions;

namespace MessageBroker.Persistence.FilePathCreators;

public class FixedFilePathCreator : IFilePathCreator
{
    private readonly string _filePath;

    public FixedFilePathCreator(string filePath)
    {
        ArgumentNullException.ThrowIfNull(filePath);
        
        _filePath = filePath;
    }
    
    public string CreatePath()
    {
        return _filePath;
    }
}