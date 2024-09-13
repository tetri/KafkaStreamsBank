using System.Collections.Concurrent;

using Confluent.Kafka;

using Microsoft.AspNetCore.Mvc;

namespace BankAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BalanceController : ControllerBase
    {
        private readonly ConsumerConfig _config;

        private readonly ConcurrentDictionary<string, double> _balances = new();

        public BalanceController()
        {
            _config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "balance-api-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        [HttpGet("{userId}")]
        public async Task<IActionResult> GetBalance(string userId)
        {
            using var consumer = new ConsumerBuilder<Null, string>(_config).Build();
            consumer.Subscribe("account-balances-topic");

            while (true)
            {
                var consumeResult = consumer.Consume();
                var message = consumeResult.Message.Value;

                // Parse o saldo recebido (message = "{userId:..., balance:...}")
                var parts = message.Split(',');
                var currentUserId = parts[0].Split(':')[1].Trim('"');
                var balance = parts[1].Split(':')[1].Trim('}');

                if (currentUserId == userId)
                {
                    return Ok(new { UserId = currentUserId, Balance = balance });
                }
            }
        }

        [HttpGet()]
        public ActionResult<IDictionary<string, double>> GetAllBalances()
        {
            using var consumer = new ConsumerBuilder<Null, string>(_config).Build();
            consumer.Subscribe("account-balances-topic");

            // Consome mensagens do Kafka
            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1)); // Timeout de 1 segundo para evitar bloqueio

                if (consumeResult != null)
                {
                    var message = consumeResult.Message.Value;

                    // Parse o saldo recebido (message = "{userId:..., balance:...}")
                    var parts = message.Split(',');
                    var userId = parts[0].Split(':')[1].Trim('"');
                    var balance = double.Parse(parts[1].Split(':')[1].Trim('}'));

                    // Atualiza o saldo no dicionário
                    _balances[userId] = balance;
                }

                return Ok(_balances);
            }
            catch (ConsumeException ex)
            {
                return StatusCode(500, $"Erro ao consumir mensagens: {ex.Message}");
            }
        }
    }
}