using MessageBroker.Api.Infrastructure.ExceptionHandlers;
using MessageBroker.Engine.Configurations;

namespace MessageBroker.Api;

public static class DependencyInjection
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddApi()
        {
            services.AddMessageBroker();
            
            services.AddExceptionHandling();
            
            services.AddControllers();

            return services;
        }

        private void AddExceptionHandling()
        {
            services.AddProblemDetails(options =>
            {
                options.CustomizeProblemDetails = context =>
                {
                    context.ProblemDetails.Instance =
                        $"{context.HttpContext.Request.Method} {context.HttpContext.Request.Path}";
                    
                    context.ProblemDetails.Extensions.TryAdd("requestId", context.HttpContext.TraceIdentifier);
                };
            });

            services.AddExceptionHandler<GlobalExceptionHandler>();
        }
    }
}