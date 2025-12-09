using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;
using MessageBroker.Core.ExpiredMessagePolicies;
using MessageBroker.Core.Queues;
using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.BrokerEngines;
using MessageBroker.Engine.Decorators.Wal;
using MessageBroker.Engine.RequeueServices;
using MessageBroker.Engine.ShutdownServices;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using MessageBroker.Persistence.FilePathCreators;
using MessageBroker.Persistence.Manifests;
using MessageBroker.Persistence.Recovery;
using MessageBroker.Persistence.WalReaders;
using MessageBroker.Persistence.WriteAheadLogs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MessageBroker.Engine.Configurations;

public static class DependencyInjection
{
    private const string EnqueueKey = "Enqueue";
    private const string AckKey = "Ack";
    private const string DeadKey = "Dead";
    private const string MergedKey = "Merged";
    
    extension(IServiceCollection services)
    {
        public void AddMessageBroker()
        {
            services.AddMessageBrokerOptions();
            
            services.TryAddSingleton(TimeProvider.System);
            services.AddSingleton<ICriticalErrorService, GracefulShutdownService>();
            services.AddSingleton<ICrcProvider, CrcProvider>();
            
            services.AddSingleton(CreateReader<EnqueueWalEvent>);
            services.AddSingleton(CreateReader<AckWalEvent>);
            services.AddSingleton(CreateReader<DeadWalEvent>);

            services.AddSingleton<IManifestManager, ManifestManager>();

            services.AddSingleton<IMessageQueueFactory, MessageQueueFactory>();

            services.AddTransient<IRecoveryService, RecoveryService>();

            services.AddSingleton<IMessageQueue>(sp =>
            {
                IRecoveryService recoveryService = sp.GetRequiredService<IRecoveryService>();

                return recoveryService.Recover();
            });
            
            services.AddFilePathCreator(EnqueueKey);
            services.AddFilePathCreator(AckKey);
            services.AddFilePathCreator(DeadKey);
            services.AddFilePathCreator(MergedKey);
            
            services.AddFileAppenders();
            
            services.AddWriteAheadLog();
            
            services.AddSingleton<IExpiredMessagePolicy>(sp =>
            {
                TimeProvider timeProvider = sp.GetRequiredService<TimeProvider>();
                BrokerOptions options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;

                return new ExpiredMessagePolicy(options.ExpiredPolicy.ExpirationTime, timeProvider);
            });
            
            services.AddBrokerEngine();
            
            services.AddRequeueService();
        }

        private void AddMessageBrokerOptions()
        {
            services.ConfigureOptions<MessageBrokerOptionsSetup>();

            services.AddSingleton<IOptions<WalOptions>>(sp =>
            {
                MessageBrokerOptions options = sp.GetRequiredService<IOptions<MessageBrokerOptions>>().Value;

                return Options.Create(options.Wal);
            });

            services.AddSingleton<WalOptions>(sp => 
                sp.GetRequiredService<IOptions<WalOptions>>().Value);
            
            services.AddSingleton<IOptions<BrokerOptions>>(sp =>
            {
                MessageBrokerOptions options = sp.GetRequiredService<IOptions<MessageBrokerOptions>>().Value;

                return Options.Create(options.Broker);
            });
            
            services.AddSingleton<BrokerOptions>(sp => 
                sp.GetRequiredService<IOptions<BrokerOptions>>().Value);
            
            services.AddSingleton<IOptions<MessageQueueOptions>>(sp =>
            {
                BrokerOptions options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;

                return Options.Create(options.Queue);
            });
            
            services.AddSingleton<MessageQueueOptions>(sp =>
                sp.GetRequiredService<IOptions<MessageQueueOptions>>().Value);
            
            services.AddSingleton<IOptions<MessageOptions>>(sp =>
            {
                BrokerOptions options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;

                return Options.Create(options.Message);
            });
            
            services.AddSingleton<MessageOptions>(sp =>
                sp.GetRequiredService<IOptions<MessageOptions>>().Value);
        }

        private void AddFilePathCreator(string key)
        {
            services.AddKeyedSingleton<IFilePathCreator>(key, (sp, _) =>
            {
                WalOptions walOptions = sp.GetRequiredService<IOptions<WalOptions>>().Value;
                TimeProvider timeProvider = sp.GetRequiredService<TimeProvider>();

                string prefix = key switch
                {
                    EnqueueKey => walOptions.FileNaming.EnqueuePrefix,
                    AckKey => walOptions.FileNaming.AckPrefix,
                    DeadKey => walOptions.FileNaming.DeadPrefix,
                    MergedKey => walOptions.FileNaming.MergePrefix,
                    _ => throw new Exception("Unknown binding key.")
                };

                return new FilePathCreator(
                    walOptions.Directory, 
                    prefix,
                    walOptions.FileNaming.Extension,
                    timeProvider);
            });
        }

        private void AddFileAppenders()
        {
            services.AddKeyedSingleton(EnqueueKey, CreateAppender<EnqueueWalEvent>);
            services.AddKeyedSingleton(AckKey, CreateAppender<AckWalEvent>);
            services.AddKeyedSingleton(DeadKey, CreateAppender<DeadWalEvent>);
            services.AddKeyedSingleton(MergedKey, CreateAppender<EnqueueWalEvent>);

            services.AddSingleton<IFileAppender<EnqueueWalEvent>>(sp =>
                sp.GetRequiredKeyedService<IFileAppender<EnqueueWalEvent>>(EnqueueKey));
            
            services.AddSingleton<IFileAppender<AckWalEvent>>(sp =>
                sp.GetRequiredKeyedService<IFileAppender<AckWalEvent>>(AckKey));
            
            services.AddSingleton<IFileAppender<DeadWalEvent>>(sp =>
                sp.GetRequiredKeyedService<IFileAppender<DeadWalEvent>>(DeadKey));
        }

        private void AddWriteAheadLog()
        {
            services.AddSingleton<WriteAheadLogManager>();

            services.AddSingleton<IWriteAheadLog>(sp =>
            {
                IWriteAheadLog innerWal = sp.GetRequiredService<WriteAheadLogManager>();
                ICriticalErrorService criticalErrorService = sp.GetRequiredService<ICriticalErrorService>();

                return new CriticalErrorWalDecorator(innerWal, criticalErrorService);
            });
        }

        private void AddBrokerEngine()
        {
            services.AddSingleton<BrokerEngine>(sp =>
            {
                TimeProvider timeProvider = sp.GetRequiredService<TimeProvider>();
                IMessageQueue messageQueue = sp.GetRequiredService<IMessageQueue>();
                IWriteAheadLog wal = sp.GetRequiredService<IWriteAheadLog>();
                IExpiredMessagePolicy expiredPolicy = sp.GetRequiredService<IExpiredMessagePolicy>();
                BrokerOptions options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;

                int maxPayloadSize = options.Message.MaxPayloadSize;
                int maxDeliveryAttempts = options.Message.MaxDeliveryAttempts;
                
                return new BrokerEngine(
                    messageQueue,
                    wal,
                    expiredPolicy,
                    timeProvider,
                    maxPayloadSize,
                    maxDeliveryAttempts);
            });

            services.AddSingleton<IBrokerEngine>(sp =>
            {
                IBrokerEngine inner = sp.GetRequiredService<BrokerEngine>();
                var logger = sp.GetRequiredService<ILogger<BrokerEngineLoggingDecorator>>();
                
                return new BrokerEngineLoggingDecorator(inner, logger);
            });

            services.AddSingleton<IAsyncBrokerEngine, AsyncBrokerEngine>();
        }

        private void AddRequeueService()
        {
            services.AddSingleton<IRequeueService>(sp =>
                sp.GetRequiredService<BrokerEngine>());
            
            services.AddHostedService<RequeueBackgroundService>(sp =>
            {
                IRequeueService requeueService = sp.GetRequiredService<IRequeueService>();
                BrokerOptions options = sp.GetRequiredService<IOptions<BrokerOptions>>().Value;
                var logger = sp.GetService<ILogger<RequeueBackgroundService>>();

                TimeSpan interval = options.Requeue.RequeueInterval;

                return new RequeueBackgroundService(requeueService, interval, logger);
            });
        }
    }
    
    private static IWalReader<TEvent> CreateReader<TEvent>(IServiceProvider sp) 
        where TEvent : WalEvent
    {
        ICrcProvider crcProvider = sp.GetRequiredService<ICrcProvider>();

        return typeof(TEvent) switch
        {
            Type t when t == typeof(EnqueueWalEvent) => (IWalReader<TEvent>)new EnqueueWalReader(crcProvider),
            Type t when t == typeof(AckWalEvent) => (IWalReader<TEvent>)new AckWalReader(crcProvider),
            Type t when t == typeof(DeadWalEvent) => (IWalReader<TEvent>)new DeadWalReader(crcProvider),
            _ => throw new Exception("Unknown WalEvent type."),
        };
    }

    private static IFileAppender<TEvent> CreateAppender<TEvent>(IServiceProvider sp, object? key)
        where TEvent : WalEvent
    {
        WalOptions walOptions = sp.GetRequiredService<IOptions<WalOptions>>().Value;
        ICrcProvider crcProvider = sp.GetRequiredService<ICrcProvider>();
        IFilePathCreator pathCreator = sp.GetRequiredKeyedService<IFilePathCreator>(key);
        
        int maxWrites = walOptions.MaxWriteCountPerFile;

        return typeof(TEvent) switch
        {
            Type t when t == typeof(EnqueueWalEvent)
                => (IFileAppender<TEvent>)new EnqueueFileAppender(crcProvider, pathCreator, maxWrites),
            
            Type t when t == typeof(AckWalEvent)
                => (IFileAppender<TEvent>)new AckFileAppender(crcProvider, pathCreator, maxWrites),
            
            Type t when t == typeof(DeadWalEvent)
                => (IFileAppender<TEvent>)new DeadFileAppender(crcProvider, pathCreator, maxWrites),
            
            _ => throw new Exception("Unknown WalEvent type.")
        };
    }
}