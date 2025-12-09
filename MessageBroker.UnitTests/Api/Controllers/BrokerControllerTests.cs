using MessageBroker.Api.Controllers;
using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Microsoft.Extensions.Time.Testing;
using Moq;
using Shouldly;

namespace MessageBroker.UnitTests.Api.Controllers;

public class BrokerControllerTests
{
    private readonly Mock<IAsyncBrokerEngine> _engineMock;

    public BrokerControllerTests()
    {
        _engineMock = new();
    }

    [Fact]
    public async Task Publish_ReturnsCreated()
    {
        // Arrange
        byte[] expectedPayload = [0x10, 0x20, 0x30];
        using MemoryStream stream = new(expectedPayload);
        
        BrokerController sut = CreateSut();
        
        sut.ControllerContext.HttpContext.Request.Body = stream;

        // Act
        IActionResult actual = await sut.PublishMessage();

        // Assert
        actual.ShouldBeOfType<CreatedResult>();
    }

    [Fact]
    public async Task Publish_CallsPublishEngineMethod()
    {
        // Arrange
        byte[] expectedPayload = [0x10, 0x20, 0x30];
        using MemoryStream stream = new(expectedPayload);
        
        BrokerController sut = CreateSut();
        
        sut.ControllerContext.HttpContext.Request.Body = stream;

        // Act
        await sut.PublishMessage();

        // Assert
        _engineMock.Verify(e => e.PublishAsync(
                It.Is<byte[]>(b => ((IEnumerable<byte>)b).SequenceEqual(expectedPayload))), 
            Times.Once);
    }
    
    [Fact]
    public async Task Consume_ReturnsFile_WhenMessageExists()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message message = Message.Create(Guid.CreateVersion7(), [0x01, 0x02], 5, timeProvider);

        _engineMock.Setup(e => e.ConsumeAsync())
            .Returns(Task.FromResult(message)!);

        BrokerController sut = CreateSut();

        // Act
         IActionResult actual = await sut.ConsumeMessage();
         
        // Assert
        actual.ShouldBeOfType<FileContentResult>();
        
        FileContentResult fileResult = (FileContentResult)actual;
        
        fileResult.FileContents.ShouldBe(message.Payload);
        fileResult.ContentType.ShouldBe("application/octet-stream");
    }

    [Fact]
    public async Task Consume_ReturnsHeaders_WhenMessageExists()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message message = Message.Create(Guid.CreateVersion7(), [0x01, 0x02], 5, timeProvider);

        _engineMock.Setup(e => e.ConsumeAsync())
            .Returns(Task.FromResult(message)!);

        BrokerController sut = CreateSut();

        // Act
        await sut.ConsumeMessage();
         
        // Assert
        IHeaderDictionary actual = sut.Response.Headers;

        actual.TryGetValue("X-Message-Id", out StringValues actualId);
        actualId.ToString().ShouldBe(message.Id.ToString());
        
        actual.TryGetValue("X-Delivery-Attempts", out StringValues actualDeliveryAttempts);
        actualDeliveryAttempts.ToString().ShouldBe(message.DeliveryCount.ToString());
    }

    [Fact]
    public async Task Consume_ReturnsNoContent_WhenQueueIsEmpty()
    {
        // Arrange
        _engineMock.Setup(e => e.ConsumeAsync())
            .Returns(Task.FromResult((Message)null!));

        BrokerController sut = CreateSut();
        
        // Act
        IActionResult actual = await sut.ConsumeMessage();
        
        // Assert
        actual.ShouldBeOfType<NoContentResult>();
    }

    [Fact]
    public async Task Consume_CallsConsumeEngineMethod()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        Message message = Message.Create(Guid.CreateVersion7(), [0x01, 0x02], 5, timeProvider);

        _engineMock.Setup(e => e.ConsumeAsync())
            .Returns(Task.FromResult(message)!);

        BrokerController sut = CreateSut();

        // Act
        await sut.ConsumeMessage();
        
        // Assert
        _engineMock.Verify(e => e.ConsumeAsync(), Times.Once);
    }

    [Fact]
    public async Task Ack_ReturnsOk()
    {
        // Arrange
        Guid messageId = Guid.NewGuid();
        BrokerController sut = CreateSut();

        // Act
        IActionResult actual = await sut.AckMessage(messageId);

        // Assert
        actual.ShouldBeOfType<OkResult>();
    }

    [Fact]
    public async Task Ack_CallsAckEngineMethod()
    {
        // Arrange
        Guid messageId = Guid.NewGuid();
        BrokerController sut = CreateSut();

        // Act
        await sut.AckMessage(messageId);
        
        // Assert
        _engineMock.Verify(e => e.AckAsync(messageId));
    }
    
    private BrokerController CreateSut()
    {
        return new(_engineMock.Object)
        {
            ControllerContext = new ControllerContext()
            {
                HttpContext = new DefaultHttpContext()
            }
        };
    } 
}