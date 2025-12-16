using MessageBroker.EndToEndTests.Abstractions;

namespace MessageBroker.EndToEndTests.BrokerProcesses;

public class BrokerProcessFactory
{
    private const string UseDockerEnv = "USE_DOCKER";
    private const string ConfigSeparator = ":";
    private const string EnvironmentSeparator = "__";
    
    public IBrokerProcess Create(
        string? hostDirectory = null,
        bool resetOnStart = false,
        Dictionary<string, string?>? envVars = null)
    {
        envVars = NormalizeOptions(envVars);
        
        return IsDockerAvailable() 
            ? new DockerBrokerProcess(hostDirectory, resetOnStart, envVars)
            : new InMemoryBrokerProcess(hostDirectory, resetOnStart, envVars);
    }

    private bool IsDockerAvailable()
    {
        string? useDocker = Environment.GetEnvironmentVariable(UseDockerEnv)?.Trim();
        return string.Equals(useDocker, "true", StringComparison.OrdinalIgnoreCase);
    }

    private Dictionary<string, string?>? NormalizeOptions(Dictionary<string, string?>? envVars)
    {
        if (envVars is null)
        {
            return envVars;
        }

        (string current, string target) = IsDockerAvailable()
            ? (ConfigSeparator, EnvironmentSeparator)
            : (EnvironmentSeparator, ConfigSeparator);

        Dictionary<string, string?> normalized = [];

        foreach (var (key, value) in envVars)
        {
            string configKey = key.Replace(current, target);
            normalized[configKey] = value;
        }
        
        return normalized;
    }
}