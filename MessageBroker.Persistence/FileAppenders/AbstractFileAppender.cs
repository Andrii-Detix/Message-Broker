using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders.Exceptions;

namespace MessageBroker.Persistence.FileAppenders;

public abstract class AbstractFileAppender<TEvent> 
    : IFileAppender<TEvent>, IDisposable
    where TEvent : WalEvent
{
    private readonly Lock _streamLocker = new();
    
    private readonly IFilePathCreator _filePathCreator;
    private readonly int _maxWriteCountPerFile;

    private FileStream _fileStream;
    private int _writeCount = 0;
    private bool _isDisposed = false;

    protected AbstractFileAppender(
        IFilePathCreator? filePathCreator,
        int maxWriteCountPerFile)
    {
        ArgumentNullException.ThrowIfNull(filePathCreator);
        
        if (maxWriteCountPerFile < 1)
        {
            throw new MaxWriteCountPerFileInvalidException();
        }
        
        string path = filePathCreator.CreatePath();
        FileStream fileStream = CreateFileStream(path);
        
        CurrentFile = path;
        _fileStream = fileStream;
        _filePathCreator = filePathCreator;
        _maxWriteCountPerFile = maxWriteCountPerFile;
    }
    
    public abstract void Append(TEvent evt);

    public string CurrentFile { get; private set; }
    
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            lock (_streamLocker)
            {
                if (_isDisposed)
                {
                    return;
                }
                
                _fileStream.Dispose();
                CurrentFile = String.Empty;
                _isDisposed = true;
            }
        }
    }

    protected void SaveData(ReadOnlySpan<byte> data)
    {
        lock (_streamLocker)
        {
            if (_isDisposed)
            {
                throw new FileAppenderDisposedException();
            }
            
            if (_writeCount >= _maxWriteCountPerFile)
            {
                Rotate();
            }
            
            _fileStream.Write(data);
            _fileStream.Flush();
            _writeCount++;
        }
    }

    private void Rotate()
    {
        _fileStream.Dispose();
        string path = _filePathCreator.CreatePath();
        _fileStream = CreateFileStream(path);
        CurrentFile = path;
        _writeCount = 0;
    }

    private FileStream CreateFileStream(string path)
    {
        string? directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
        
        return new FileStream(
            path,
            FileMode.Append,
            FileAccess.Write,
            FileShare.None,
            4096,
            useAsync: false);
    }
}