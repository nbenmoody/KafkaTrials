using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Service.Producer.Models;

namespace Service.Producer.Services
{
    public class KafkaProducerService
    {
        private readonly string _bootstrapServers;
        private readonly ILogger<KafkaProducerService> _logger;
        private readonly string _topicName;

        public KafkaProducerService(string bootstrapServers, string topicName, ILogger<KafkaProducerService> logger)
        {
            _bootstrapServers = bootstrapServers;
            _topicName = topicName;
            _logger = logger;
        }

        public async Task CreateTopicIfNotExistsAsync()
        {
            try
            {
                using var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = _bootstrapServers
                }).Build();

                // Check if topic exists
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                var topicExists = metadata.Topics.Exists(t => t.Topic == _topicName);

                if (!topicExists)
                {
                    _logger.LogInformation($"Creating topic: {_topicName}");
                    await adminClient.CreateTopicsAsync(new TopicSpecification[]
                    {
                        new TopicSpecification
                        {
                            Name = _topicName,
                            ReplicationFactor = 1,
                            NumPartitions = 1
                        }
                    });
                    _logger.LogInformation($"Topic {_topicName} created successfully");
                }
                else
                {
                    _logger.LogInformation($"Topic {_topicName} already exists");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error creating topic {_topicName}");
                throw;
            }
        }

        public async Task ProduceMessageAsync(Message message)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = _bootstrapServers,
                    ClientId = Dns.GetHostName()
                };

                using var producer = new ProducerBuilder<string, string>(config).Build();
                
                var messageJson = JsonSerializer.Serialize(message);
                
                var result = await producer.ProduceAsync(_topicName, new Message<string, string>
                {
                    Key = message.Id.ToString(),
                    Value = messageJson
                });

                _logger.LogInformation($"Message delivered to {result.TopicPartitionOffset}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error producing message to Kafka");
                throw;
            }
        }
    }
}
