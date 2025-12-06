using MessageBroker.Persistence.Manifests;

namespace MessageBroker.Persistence.Abstractions;

public interface IManifestManager
{
    WalManifest Load();
    
    void Save(WalManifest manifest);
    
    WalFiles LoadWalFiles();
}