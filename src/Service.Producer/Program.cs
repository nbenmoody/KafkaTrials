using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Service.Producer.Services;

class Program {
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
    
        builder.Services.AddOpenApi();
        
        var kafkaConfig = builder.Configuration.GetSection("Kafka");
        var bootstrapServers = kafkaConfig["BootstrapServers"] ?? "kafka:29092";
        var topicName = kafkaConfig["TopicName"] ?? "example-messages";
        
        builder.Services.AddSingleton(sp => new KafkaProducerService(
            bootstrapServers,
            topicName,
            sp.GetRequiredService<ILogger<KafkaProducerService>>()));
        
        builder.Services.AddHostedService<MessagePublisherService>();
    
        var app = builder.Build();
    
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
        }
    
        app.UseHttpsRedirection();
    
        

        MapEndpoints(app, kafkaService);
    
        app.Run();
    }

    private static void MapEndpoints(WebApplication app, KafkaProducerService kafkaService) {
        app.MapPost("/publish", async (string content, KafkaProducerService kafkaService) =>
        {
            var message = new Service.Producer.Models.Message
            {
                Content = content
            };
            
            await kafkaService.ProduceMessageAsync(message);
            
            return Results.Ok(new { MessageId = message.Id, message.Content, message.Timestamp });
        })
        .WithName("PublishMessage");

        app.MapGet("/health", () =>
        {
            return Results.Ok;
        })
        .WithName("Health");

    }
}