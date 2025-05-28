using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Service.Producer.Models;

namespace Service.Producer.Services
{
    public class MessagePublisherService : BackgroundService
    {
        private readonly KafkaProducerService _kafkaProducerService;
        private readonly ILogger<MessagePublisherService> _logger;
        private readonly Random _random = new Random();
        private readonly string[] _sampleMessages = new[]
        {
            "Message 1",
            "Message 2",
            "Message 3",
            "Message 4",
            "Message 5",
            "Message 6",
            "Message 7",
            "Message 8",
            "Message 9",
            "Message 10"
        };

        public MessagePublisherService(KafkaProducerService kafkaProducerService, ILogger<MessagePublisherService> logger)
        {
            _kafkaProducerService = kafkaProducerService;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Ensure the topic exists before we start publishing
            await _kafkaProducerService.CreateTopicIfNotExistsAsync();
            
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var message = new Message
                    {
                        Content = _sampleMessages[_random.Next(_sampleMessages.Length)]
                    };

                    _logger.LogInformation($"Publishing message: {message.Id} - {message.Content}");
                    await _kafkaProducerService.ProduceMessageAsync(message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred while publishing message");
                }

                // Wait for a random interval between 1-5 seconds before sending the next message
                await Task.Delay(TimeSpan.FromSeconds(_random.Next(1, 6)), stoppingToken);
            }
        }
    }
}
