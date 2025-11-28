using System.Globalization;
using System.Text;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.FilePathCreators.Exceptions;

namespace MessageBroker.Persistence.FilePathCreators;

public class FilePathCreator : IFilePathCreator
{
    private const char NameSeparator = '-';
    private const char ExtensionSeparator = '.';
    private const int StartSegmentNumber = 1;
    
    private readonly string _directoryPath;
    private readonly string _prefix;
    private readonly string _extension;
    private readonly TimeProvider _timeProvider;

    private DateTimeOffset _timestamp;
    private int _segmentNumber = 0;

    public FilePathCreator(string? directoryPath, string? prefix, string? extension, TimeProvider? timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        
        _directoryPath = directoryPath?.Trim() ?? string.Empty;

        prefix = prefix?.Trim();
        
        if (string.IsNullOrEmpty(prefix))
        {
            throw new FileNamePrefixEmptyException();
        }

        extension = extension?.Trim().Trim(ExtensionSeparator);
        
        if (string.IsNullOrEmpty(extension))
        {
            throw new FileNameExtensionEmptyException();
        }
        
        _prefix = prefix;
        _extension = extension;
        _timeProvider = timeProvider;
        _timestamp = timeProvider.GetUtcNow();
    }
    
    public string CreatePath()
    {
        UpdateCreationData();

        StringBuilder builder = new();

        builder.Append(_prefix);

        builder.Append(NameSeparator);
        builder.Append(_timestamp.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture));

        builder.Append(NameSeparator);
        builder.Append(_segmentNumber);

        builder.Append(ExtensionSeparator);
        builder.Append(_extension);
        
        string fileName = builder.ToString();
        
        return Path.Combine(_directoryPath, fileName);
    }

    private void UpdateCreationData()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        
        long nowUnixTime = now.ToUnixTimeSeconds();
        long timestampUnixTime = _timestamp.ToUnixTimeSeconds();

        if (nowUnixTime == timestampUnixTime)
        {
            _segmentNumber++;
            return;
        }
        
        _timestamp = now;
        _segmentNumber = StartSegmentNumber;
    }
}