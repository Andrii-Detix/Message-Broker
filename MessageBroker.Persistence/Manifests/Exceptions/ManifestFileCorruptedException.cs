using MessageBroker.Persistence.Common.Exceptions;

namespace MessageBroker.Persistence.Manifests.Exceptions;

public class ManifestFileCorruptedException()
    : WalStorageException("Manifest file is corrupted.");