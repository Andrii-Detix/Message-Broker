using MessageBroker.Api;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApi();

var app = builder.Build();

app.UseExceptionHandler();

app.MapControllers();

app.Run();