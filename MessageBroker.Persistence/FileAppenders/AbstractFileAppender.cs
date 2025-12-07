using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders.Exceptions;

namespace MessageBroker.Persistence.FileAppenders;

public abstract class AbstractFileAppender<TEvent> 
    : IFileAppender<TEvent>, IDisposable
    where TEvent : WalEvent
{
    private readonly Lock _streamLocker = new();
    
    private readonly ICrcProvider _crcProvider;
    private readonly IFilePathCreator _filePathCreator;
    private readonly int _maxWriteCountPerFile;

    private FileStream _fileStream;
    private int _writeCount = 0;
    private volatile bool _isDisposed = false;

    protected AbstractFileAppender(
        ICrcProvider? crcProvider,
        IFilePathCreator? filePathCreator,
        int maxWriteCountPerFile)
    {
        ArgumentNullException.ThrowIfNull(crcProvider);
        ArgumentNullException.ThrowIfNull(filePathCreator);
        
        if (maxWriteCountPerFile < 1)
        {
            throw new MaxWriteCountPerFileInvalidException();
        }
        
        string path = filePathCreator.CreatePath();
        FileStream fileStream = CreateFileStream(path);
        
        CurrentFile = path;
        _fileStream = fileStream;
        _crcProvider = crcProvider;
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
        if (_isDisposed)
        {
            throw new FileAppenderDisposedException();
        }
        
        lock (_streamLocker)
        {
            if (_isDisposed)
            {
                throw new FileAppenderDisposedException();
            }
            
            try
            {
                if (_writeCount >= _maxWriteCountPerFile)
                {
                    Rotate();
                }
            
                WriteToStream(data);
            }
            catch
            {
                try
                {
                    Rotate();
                    WriteToStream(data);
                }
                catch
                {
                    Dispose();
                    throw;
                }
            }
        }
    }

    private void WriteToStream(ReadOnlySpan<byte> data)
    {
        int headerSize = _crcProvider.HeaderSize;
        Span<byte> header = stackalloc byte[headerSize];
        _crcProvider.WriteHeader(header, data);
            
        _fileStream.Write(header);
        _fileStream.Write(data);
            
        _fileStream.Flush();
        _writeCount++;
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