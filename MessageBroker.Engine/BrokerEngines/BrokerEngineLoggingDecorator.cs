using MessageBroker.Core.Messages.Models;
using MessageBroker.Engine.Abstractions;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Engine.BrokerEngines;

public class BrokerEngineLoggingDecorator(
    IBrokerEngine innerEngine,
    ILogger<BrokerEngineLoggingDecorator> logger)
    : IBrokerEngine
{
    public void Publish(byte[] payload)
    {
        try
        {
            logger.LogDebug("Start publishing message.");
            
            innerEngine.Publish(payload);
            
            logger.LogDebug("Publishing message completed successfully.");
        }
        catch (Exception e)
        {
            logger.LogError(e, "Publishing message failed with error '{ErrorMessage}'.", e.Message);
            
            throw;
        }
    }

    public Message? Consume()
    {
        try
        {
            logger.LogDebug("Start consuming message.");
            
            Message? message = innerEngine.Consume();
            
            if (message is not null)
            {
                logger.LogDebug("Consumed message '{MessageId}' successfully.", message.Id);
            }
            else
            {
                logger.LogDebug("Consume returned no message.");
            }
            
            return message;
        }
        catch (Exception e)
        {
            logger.LogError(e, "Consuming message failed with error '{ErrorMessage}'.", e.Message);
            
            throw;
        }
    }

    public void Ack(Guid messageId)
    {
        try
        {
            logger.LogDebug("Start acknowledging message with id '{MessageId}'.", messageId);
            
            innerEngine.Ack(messageId);
            
            logger.LogDebug("Acknowledging message completed successfully.");
        }
        catch (Exception e)
        {
            logger.LogError(
                e, 
                "Acknowledging message with id '{MessageId}' failed with error '{ErrorMessage}'.", 
                messageId, 
                e.Message);
            
            throw;
        }
    }
}