using Xunit.Abstractions;
using Xunit.Sdk;

namespace MessageBroker.LoadTests.Attributes;

public class LoadTestDiscoverer : ITraitDiscoverer
{
    public IEnumerable<KeyValuePair<string, string>> GetTraits(IAttributeInfo traitAttribute)
    {
        yield return new KeyValuePair<string, string>("Category", "Load");
    }
}

[TraitDiscoverer("MessageBroker.LoadTests.Attributes.LoadTestDiscoverer", "MessageBroker.LoadTests")]
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false)]
public class LoadTestAttribute : Attribute, ITraitAttribute;