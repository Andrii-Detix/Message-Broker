using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;
using Microsoft.AspNetCore.Mvc;

namespace MessageBroker.Api.Controllers;

[ApiController]
[Route("api/broker")]
public class BrokerController(IAsyncBrokerEngine brokerEngine) 
    : ControllerBase
{
    [HttpPost("publish")]
    [Consumes("application/octet-stream")]
    public async Task<IActionResult> PublishMessage()
    {
        using MemoryStream memoryStream = new();

        await Request.Body.CopyToAsync(memoryStream);

        byte[] payload = memoryStream.ToArray();
        
        await brokerEngine.PublishAsync(payload);

        return Created();
    }

    [HttpGet("consume")]
    [Produces("application/octet-stream")]
    public async Task<IActionResult> ConsumeMessage()
    {
        Message? message = await brokerEngine.ConsumeAsync();

        if (message is null)
        {
            return NoContent();
        }
        
        Response.Headers.Append("X-Message-Id", message.Id.ToString());
        Response.Headers.Append("X-Delivery-Attempts", message.DeliveryCount.ToString());
        
        return File(message.Payload, "application/octet-stream");
    }

    [HttpPost("ack/{id:guid}")]
    public async Task<IActionResult> AckMessage(Guid id)
    {
        await brokerEngine.AckAsync(id);

        return Ok();
    }
}