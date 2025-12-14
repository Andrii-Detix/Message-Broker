using MessageBroker.Core.Messages.Models;
using Microsoft.Extensions.Time.Testing;

namespace MessageBroker.UnitTests.Helpers;

public static class MessageHelper
{
    public static Message CreateMessage(
        Guid? messageId = null,
        byte[]? payload = null,
        int? maxDeliveryAttempts = null,
        TimeProvider? timeProvider = null)
    {
        return Message.Create(
            messageId ?? Guid.CreateVersion7(), 
            payload ?? [], 
            maxDeliveryAttempts ?? 5, 
            timeProvider ?? new FakeTimeProvider());
    }

    public static Message[] CreateMessages(int count, Guid? fixedId = null)
    {
        return Enumerable
            .Range(0, count)
            .Select(_ => CreateMessage(messageId: fixedId ?? Guid.CreateVersion7()))
            .ToArray();
    }
}