using Shouldly;

namespace MessageBroker.EndToEndTests.Extensions;

public static class HttpResponseExtensions
{
    extension(HttpResponseMessage response)
    {
        public string? ShouldHaveHeader(string headerName)
        {
            response.Headers.TryGetValues(headerName, out var values)
                .ShouldBeTrue();
        
            return values.FirstOrDefault();
        }
        
        public void ShouldHaveHeader(string headerName, string expectedValue)
        {
            string? actualValue = response.ShouldHaveHeader(headerName);
            actualValue.ShouldBe(expectedValue);
        }
    }
}