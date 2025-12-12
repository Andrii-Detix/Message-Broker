FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY Directory.Packages.props .
COPY MessageBroker.Api/MessageBroker.Api.csproj MessageBroker.Api/
RUN dotnet restore MessageBroker.Api/MessageBroker.Api.csproj
COPY . .
RUN dotnet publish MessageBroker.Api/MessageBroker.Api.csproj -c $BUILD_CONFIGURATION -o /publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /publish .
ENTRYPOINT ["dotnet", "MessageBroker.Api.dll"]