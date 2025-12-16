using System.Diagnostics;
using System.Runtime.InteropServices;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MessageBroker.EndToEndTests.Abstractions;

namespace MessageBroker.EndToEndTests.BrokerProcesses;

public class DockerBrokerProcess(
    string? hostDirectory = null,
    bool resetOnStart = false,
    Dictionary<string, string?>? envVars = null) 
    : IBrokerProcess
{
    private const string ImageName = "message-broker:test";
    private const string MountPoint = "/data";
    private const string WalDirectory = "/data/wal";
    private const int InternalPort = 8080;

    private readonly IContainer _container = Create(hostDirectory, resetOnStart, envVars);
    
    public async Task StartAsync()
    {
        await _container.StartAsync();
    }

    public async Task StopAsync()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }

    public HttpClient CreateClient()
    {
        int port = _container.GetMappedPublicPort(InternalPort);
        return new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{port}")
        };
    }
    
    private static IContainer Create(
        string? hostDirectory = null,
        bool resetOnStart = false,
        Dictionary<string, string?>? envVars = null)
    {
        ContainerBuilder builder = new ContainerBuilder()
            .WithImage(ImageName)
            .WithPortBinding(InternalPort, true)
            .WithEnvironment("MessageBroker__Wal__Directory", WalDirectory)
            .WithEnvironment("MessageBroker__Wal__ResetOnStartup", resetOnStart.ToString())
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(InternalPort));

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            builder = builder.WithCreateParameterModifier(parameter => 
            {
                parameter.User = GetLinuxUserId(); 
            });
        }
        
        if (!string.IsNullOrWhiteSpace(hostDirectory))
        {
            builder = builder.WithBindMount(hostDirectory, MountPoint);
        }

        if (envVars is not null)
        {
            foreach (var envVar in envVars)
            {
                builder = builder.WithEnvironment(envVar.Key, envVar.Value);
            }
        }
        
        return builder.Build();
    }
    
    private static string GetLinuxUserId()
    {
        try
        {
            Process process = new()
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "id",
                    Arguments = "-u",
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };
            process.Start();
            string output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            return output.Trim();
        }
        catch
        {
            return "0";
        }
    }
}