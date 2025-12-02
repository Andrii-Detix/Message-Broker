using MessageBroker.Core.Abstractions;
using MessageBroker.Core.ExpiredMessagePolicies.Exceptions;
using MessageBroker.Core.Messages.Models;

namespace MessageBroker.Core.ExpiredMessagePolicies;

public class ExpiredMessagePolicy : IExpiredMessagePolicy
{
    private readonly TimeSpan _expirationTime;
    private readonly TimeProvider _timeProvider;

    public ExpiredMessagePolicy(
        TimeSpan expirationTime, 
        TimeProvider? timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);

        if (expirationTime <= TimeSpan.Zero)
        {
            throw new MessageExpirationTimeInvalidException();
        }
        
        _expirationTime = expirationTime;
        _timeProvider = timeProvider;
    }
    
    public bool IsExpired(Message message)
    {
        ArgumentNullException.ThrowIfNull(message);
        
        if (message.State != MessageState.Sent 
            || message.LastSentAt is null)
        {
            return false;
        }
        
        DateTimeOffset now = _timeProvider.GetUtcNow();

        TimeSpan diff = now - message.LastSentAt.Value;

        return diff >= _expirationTime;
    }
} 