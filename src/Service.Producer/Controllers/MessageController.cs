using Microsoft.AspNetCore.Mvc;
using Service.Producer.Models;
using Service.Producer.Services;

namespace Service.Producer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class MessageController : ControllerBase
    {
        private readonly KafkaProducerService _kafkaService;

        public MessageController(KafkaProducerService kafkaService)
        {
            _kafkaService = kafkaService;
        }

        [HttpPost("publish")]
        public async Task<IActionResult> PublishMessage([FromBody] string content)
        {
            var message = new Message
            {
                Content = content
            };

            await _kafkaService.ProduceMessageAsync(message);

            return Ok(new { MessageId = message.Id, message.Content, message.Timestamp });
        }
    }
}