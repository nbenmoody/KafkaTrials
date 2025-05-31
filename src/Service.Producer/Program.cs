using Service.Producer.Services;

class Program {
    public static void Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
    
        builder.Services.AddOpenApi();
        builder.Services.AddControllers();
        
        var kafkaConfig = builder.Configuration.GetSection("Kafka");
        var bootstrapServers = kafkaConfig["BootstrapServers"] ?? "kafka:29092";
        var topicName = kafkaConfig["TopicName"] ?? "example-messages";
        
        builder.Services.AddSingleton(sp => new KafkaProducerService(
            bootstrapServers,
            topicName,
            sp.GetRequiredService<ILogger<KafkaProducerService>>()));
        
        builder.Services.AddHostedService<MessagePublisherService>();
        builder.Services.AddHealthChecks();
    
        var app = builder.Build();
    
        if (app.Environment.IsDevelopment())
        {
            app.MapOpenApi();
        }
    
        app.MapControllers();
        app.MapHealthChecks("/health");
        app.Run();
    }
}