using Bogus;

namespace MessageBroker.LoadTests.Helpers;

public static class PayloadGenerator
{
    public static byte[][] Generate(int count, int minBytes = 10, int maxBytes = 2048)
    {
        Faker faker = new();

        byte[][] payloads = new byte[count][];

        for (int i = 0; i < count; i++)
        {
            int size = faker.Random.Int(minBytes, maxBytes);
            payloads[i] = faker.Random.Bytes(size);
        }
        
        return payloads;
    }
}