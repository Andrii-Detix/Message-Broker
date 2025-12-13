using System.Text.Json;
using MessageBroker.Api.Infrastructure.ExceptionHandlers;
using MessageBroker.Engine.BrokerEngines.Exceptions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Api.Infrastructure.ExceptionHandlers;

public class GlobalExceptionHandlerTests
{
    private readonly Mock<ILogger<GlobalExceptionHandler>> _loggerMock;

    public GlobalExceptionHandlerTests()
    {
        _loggerMock = new();
    }

    public static IEnumerable<object[]> ExceptionScenarios()
    {
        yield return
        [
            new ArgumentException("Bad argument"), 
            StatusCodes.Status400BadRequest
        ];
        yield return
        [
            new SentMessageNotFoundException(Guid.NewGuid()), 
            StatusCodes.Status404NotFound
        ];
        yield return
        [
            new PayloadTooLargeException(200, 100), 
            StatusCodes.Status413PayloadTooLarge
        ];
    }

    [Theory]
    [MemberData(nameof(ExceptionScenarios))]
    public async Task TryHandleAsync_ReturnsCorrectStatusCodeAndFields_WhenSpecificExceptionIsThrown(
        Exception exception, 
        int expectedStatusCode)
    {
        // Arrange
        HttpContext context = new DefaultHttpContext();
        Stream stream = new MemoryStream();
        context.Response.Body = stream;
        
        GlobalExceptionHandler sut = new(_loggerMock.Object);
        
        // Act
        bool actual = await sut.TryHandleAsync(context, exception, CancellationToken.None);
        
        // Assert
        actual.ShouldBeTrue();
        context.Response.StatusCode.ShouldBe(expectedStatusCode);
        
        stream.Seek(0, SeekOrigin.Begin);
        ProblemDetails? problemDetails = await JsonSerializer.DeserializeAsync<ProblemDetails>(stream);
        
        problemDetails.ShouldNotBeNull();
        problemDetails.Status.ShouldBe(expectedStatusCode);
        problemDetails.Type.ShouldNotBeNullOrEmpty();
        problemDetails.Title.ShouldNotBeNullOrEmpty();
        
        problemDetails.Detail.ShouldNotBeNullOrEmpty();
        problemDetails.Detail.ShouldBe(exception.Message);
    }
    
    [Fact]
    public async Task TryHandleAsync_DoesNotReturnOriginalExceptionMessage_WhenServerErrorOccurs()
    {
        // Arrange
        HttpContext context = new DefaultHttpContext();
        Stream stream = new MemoryStream();
        context.Response.Body = stream;
        
        Exception exception = new("Enqueue wal event failure.");
        GlobalExceptionHandler sut = new(_loggerMock.Object);
        
        // Act
        bool actual = await sut.TryHandleAsync(context, exception, CancellationToken.None);
        
        // Assert
        actual.ShouldBeTrue();
        context.Response.StatusCode.ShouldBe(StatusCodes.Status500InternalServerError);
        
        stream.Seek(0, SeekOrigin.Begin);
        ProblemDetails? problemDetails = await JsonSerializer.DeserializeAsync<ProblemDetails>(stream);
        
        problemDetails.ShouldNotBeNull();
        problemDetails.Status.ShouldBe(StatusCodes.Status500InternalServerError);
        
        problemDetails.Detail.ShouldNotBeNullOrEmpty();
        problemDetails.Detail.ShouldNotBe(exception.Message);
    }
}