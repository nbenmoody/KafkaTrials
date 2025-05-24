FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["Service.Producer.csproj", "./"]
RUN dotnet restore "Service.Producer.csproj"
COPY . .
RUN dotnet build "Service.Producer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Service.Producer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Service.Producer.dll"]