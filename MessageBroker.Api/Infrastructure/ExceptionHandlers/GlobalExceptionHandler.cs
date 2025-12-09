using MessageBroker.Engine.BrokerEngines.Exceptions;
using MessageBroker.Engine.Common.Exceptions;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace MessageBroker.Api.Infrastructure.ExceptionHandlers;

public class GlobalExceptionHandler(ILogger<GlobalExceptionHandler> logger) 
    : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(
        HttpContext httpContext, 
        Exception exception, 
        CancellationToken cancellationToken)
    {
        logger.LogError(exception, "Exception occurred: {Message}", exception.Message);
        
        int statusCode = GetStatusCode(exception);
        
        string detail = statusCode == StatusCodes.Status500InternalServerError
            ? "An internal server error occurred."
            : exception.Message;

        ProblemDetails problemDetails = new()
        {
            Status = statusCode,
            Title = GetTitle(statusCode),
            Detail = detail,
            Type = GetType(statusCode)
        };

        httpContext.Response.StatusCode = statusCode;
        
        await httpContext.Response.WriteAsJsonAsync(problemDetails, cancellationToken);
        
        return true;
    }

    private static int GetStatusCode(Exception exception)
    {
        return exception switch
        {
            ArgumentException or ArgumentNullException => StatusCodes.Status400BadRequest,
            SentMessageNotFoundException => StatusCodes.Status404NotFound,
            PayloadTooLargeException => StatusCodes.Status413PayloadTooLarge,
            _ => StatusCodes.Status500InternalServerError
        };
    }

    private static string GetTitle(int statusCode)
    {
        return statusCode switch
        {
            StatusCodes.Status400BadRequest => "Bad Request",
            StatusCodes.Status404NotFound => "Not Found",
            StatusCodes.Status413PayloadTooLarge => "Payload Too Large",
            _ => "Server Failure"
        };
    }

    private static string GetType(int statusCode)
    {
        return statusCode switch
        {
            StatusCodes.Status400BadRequest => "https://www.rfc-editor.org/rfc/rfc7231#section-6.5.1",
            StatusCodes.Status404NotFound => "https://www.rfc-editor.org/rfc/rfc7231#section-6.5.4",
            StatusCodes.Status413PayloadTooLarge => "https://www.rfc-editor.org/rfc/rfc7231#section-6.5.11",
            _ => "https://www.rfc-editor.org/rfc/rfc7231#section-6.6.1"
        };
    }
}