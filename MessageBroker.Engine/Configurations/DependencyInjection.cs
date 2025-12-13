using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MessageBroker.Core.Abstractions;
using MessageBroker.Core.Configurations;
using MessageBroker.Core.ExpiredMessagePolicies;
using MessageBroker.Core.Queues;
using MessageBroker.Engine.Abstractions;
using MessageBroker.Engine.BrokerEngines;
using MessageBroker.Engine.Decorators.BrokerEngine;
using MessageBroker.Engine.Decorators.Wal;
using MessageBroker.Engine.Services.Requeue;
using MessageBroker.Engine.Services.Shutdown;
using MessageBroker.Persistence.Abstractions;
using MessageBroker.Persistence.Configurations;
using MessageBroker.Persistence.CrcProviders;
using MessageBroker.Persistence.Events;
using MessageBroker.Persistence.FileAppenders;
using MessageBroker.Persistence.FilePathCreators;
using MessageBroker.Persistence.Manifests;
using MessageBroker.Persistence.Services.GarbageCollector;
using MessageBroker.Persistence.Services.Recovery;
using MessageBroker.Persistence.WalReaders;
using MessageBroker.Persistence.WriteAheadLogs;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.Configurations;

public static class DependencyInjection
{
    private const string EnqueueKey = "Enqueue";
    private const string AckKey = "Ack";
    private const string DeadKey = "Dead";
    private const string EnqueueMergedKey = "Enqueue-Merged";
    private const string AckMergedKey = "Ack-Merged";
    private const string DeadMergedKey = "Dead-Merged";

    extension(IServiceCollection services)
    {
        public IServiceCollection AddMessageBroker()
        {
            services.AddMessageBrokerOptions();
            services.AddCoreServices();
            services.AddPersistenceServices();
            services.AddBrokerEngineServices();
            services.AddBackgroundServices();

            return services;
        }

        private void AddMessageBrokerOptions()
        {
            services.ConfigureOptions<MessageBrokerOptionsSetup>();

            services.RegisterOptionValues<WalOptions, MessageBrokerOptions>(opt => opt.Wal);
            services.RegisterOptionValues<GarbageCollectorOptions, WalOptions>(opt => opt.GarbageCollector);
            services.RegisterOptionValues<BrokerOptions, MessageBrokerOptions>(opt => opt.Broker);
            services.RegisterOptionValues<MessageQueueOptions, BrokerOptions>(opt => opt.Queue);
            services.RegisterOptionValues<MessageOptions, BrokerOptions>(opt => opt.Message);
        }

        private void AddCoreServices()
        {
            services.TryAddSingleton(TimeProvider.System);
        
            services.AddSingleton<IExpiredMessagePolicy>(sp =>
            {
                BrokerOptions options = sp.GetRequiredService<BrokerOptions>();
                return new ExpiredMessagePolicy(options.ExpiredPolicy.ExpirationTime, sp.GetRequiredService<TimeProvider>());
            });
        }

        private void AddPersistenceServices()
        {
            services.AddSingleton<ICrcProvider, CrcProvider>();
            
            services.AddSingleton<IManifestManager, ManifestManager>();
            services.AddTransient<IRecoveryService, RecoveryService>();

            services.AddSingleton<IMessageQueueFactory, MessageQueueFactory>();
            services.AddSingleton<IMessageQueue>(sp => sp.GetRequiredService<IRecoveryService>().Recover());

            AddPathCreator(services, EnqueueKey, opt => opt.EnqueuePrefix);
            AddPathCreator(services, AckKey, opt => opt.AckPrefix);
            AddPathCreator(services, DeadKey, opt => opt.DeadPrefix);
            AddPathCreator(services, EnqueueMergedKey, opt => opt.EnqueueMergedPrefix);
            AddPathCreator(services, AckMergedKey, opt => opt.AckMergedPrefix);
            AddPathCreator(services, DeadMergedKey, opt => opt.DeadMergedPrefix);

            services.AddSingleton<IWalReader<EnqueueWalEvent>, EnqueueWalReader>();
            services.AddSingleton<IWalReader<AckWalEvent>, AckWalReader>();
            services.AddSingleton<IWalReader<DeadWalEvent>, DeadWalReader>();

            services.AddFileAppender<EnqueueWalEvent, EnqueueFileAppender>(EnqueueKey);
            services.AddFileAppender<AckWalEvent, AckFileAppender>(AckKey);
            services.AddFileAppender<DeadWalEvent, DeadFileAppender>(DeadKey);
        
            services.AddSingleton<IFileAppenderFactory>(sp => new FixedFileAppenderFactory(
                sp.GetRequiredService<ICrcProvider>(),
                sp.GetRequiredKeyedService<IFilePathCreator>(EnqueueMergedKey),
                sp.GetRequiredKeyedService<IFilePathCreator>(AckMergedKey),
                sp.GetRequiredKeyedService<IFilePathCreator>(DeadMergedKey)));

            services.AddSingleton<WriteAheadLogManager>();
            services.AddSingleton<IWriteAheadLog>(sp =>
            {
                IWriteAheadLog inner = sp.GetRequiredService<WriteAheadLogManager>();
                ICriticalErrorService errorService = sp.GetRequiredService<ICriticalErrorService>();
                return new CriticalErrorWalDecorator(inner, errorService);
            });
        }

        private void AddBrokerEngineServices()
        {
            services.AddSingleton<ICriticalErrorService, GracefulShutdownService>();
            
            services.AddSingleton<BrokerEngine>(sp =>
            {
                BrokerOptions options = sp.GetRequiredService<BrokerOptions>();
                return new BrokerEngine(
                    sp.GetRequiredService<IMessageQueue>(),
                    sp.GetRequiredService<IWriteAheadLog>(),
                    sp.GetRequiredService<IExpiredMessagePolicy>(),
                    sp.GetRequiredService<TimeProvider>(),
                    options.Message.MaxPayloadSize,
                    options.Message.MaxDeliveryAttempts);
            });

            services.AddSingleton<IBrokerEngine>(sp =>
            {
                BrokerEngine engine = sp.GetRequiredService<BrokerEngine>();
                var logger = sp.GetRequiredService<ILogger<BrokerEngineLoggingDecorator>>();
                return new BrokerEngineLoggingDecorator(engine, logger);
            });

            services.AddSingleton<IAsyncBrokerEngine, AsyncBrokerEngine>();
        
            services.AddSingleton<IRequeueService>(sp => sp.GetRequiredService<BrokerEngine>());
        }

        private void AddBackgroundServices()
        {
            services.AddSingleton<IWalGarbageCollectorService, WalGarbageCollectorService>();
        
            services.AddHostedService<WalGarbageCollectorBackgroundService>();
        
            services.AddHostedService<RequeueBackgroundService>(sp =>
            {
                BrokerOptions options = sp.GetRequiredService<BrokerOptions>();
                return new RequeueBackgroundService(
                    sp.GetRequiredService<IRequeueService>(),
                    options.Requeue.RequeueInterval,
                    sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<ILogger<RequeueBackgroundService>>());
            });
        }

        private void RegisterOptionValues<TTarget, TSource>(Func<TSource, TTarget> selector) 
            where TTarget : class 
            where TSource : class
        {
            services.AddSingleton<IOptions<TTarget>>(sp =>
            {
                var sourceOptions = sp.GetRequiredService<IOptions<TSource>>().Value;
                return Options.Create(selector(sourceOptions));
            });

            services.AddSingleton<TTarget>(sp => sp.GetRequiredService<IOptions<TTarget>>().Value);
        }
    }

    private static void AddPathCreator(IServiceCollection services, string key, Func<FileNamingOptions, string> prefixSelector)
    {
        services.AddKeyedSingleton<IFilePathCreator>(key, (sp, _) =>
        {
            WalOptions options = sp.GetRequiredService<WalOptions>();
            return new FilePathCreator(
                options.Directory,
                prefixSelector(options.FileNaming),
                options.FileNaming.Extension,
                sp.GetRequiredService<TimeProvider>());
        });
    }
    
    private static void AddFileAppender<TEvent, TImplementation>(this IServiceCollection services, string key)
        where TEvent : WalEvent
        where TImplementation : class, IFileAppender<TEvent>
    {
        services.AddKeyedSingleton<IFileAppender<TEvent>>(key, (sp, _) =>
        {
            WalOptions options = sp.GetRequiredService<WalOptions>();
            IFilePathCreator pathCreator = sp.GetRequiredKeyedService<IFilePathCreator>(key);
            var crc = sp.GetRequiredService<ICrcProvider>();

            return (TImplementation)Activator.CreateInstance(typeof(TImplementation), crc, pathCreator, options.MaxWriteCountPerFile)!;
        });

        services.AddSingleton<IFileAppender<TEvent>>(sp => 
            sp.GetRequiredKeyedService<IFileAppender<TEvent>>(key));
    }
}