using Confluent.Kafka;
using Service.Consumer.Models;
using System.Text.Json;

namespace Service.Consumer.Services
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly ILogger<KafkaConsumerService> _logger;
        private const string Topic = "example-messages";

        public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
        {
            _logger = logger;
            
            var config = new ConsumerConfig
            {
                BootstrapServers = "kafka:29092",
                GroupId = "consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(Topic);
            _logger.LogInformation("Kafka consumer started. Listening to topic: {Topic}", Topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(stoppingToken);
                        
                        if (consumeResult != null)
                        {
                            var message = JsonSerializer.Deserialize<Message>(consumeResult.Message.Value);
                            
                            _logger.LogInformation("Message received: {@Message}", message);
                            
                            // Process the message as needed
                            await ProcessMessageAsync(message);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error consuming message");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when stoppingToken is canceled
            }
            finally
            {
                _consumer.Close();
                _consumer.Dispose();
            }
        }

        private Task ProcessMessageAsync(Message message)
        {
            // Implement your message processing logic here
            _logger.LogInformation("Processing message: {Id}, Content: {Content}", message?.Id, message?.Content);
            return Task.CompletedTask;
        }
    }
}