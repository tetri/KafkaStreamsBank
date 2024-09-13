using BankTransactionProducer;

using Bogus;

using Confluent.Kafka;

class Program
{
    static void Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var userIds = new List<string>
        {
            "Ana Souza", "Bruno Oliveira", "Carla Costa", "Daniel Silva", "Eduarda Lima",
            "Felipe Almeida", "Gabriela Martins", "Henrique Pereira", "Isabela Rocha", "João Santos"
        };

        var faker = new Faker<BankTransaction>("pt_BR").StrictMode(true)
            .RuleFor(t => t.UserId, f => userIds[f.Random.Number(userIds.Count - 1)])
            .RuleFor(t => t.Amount, f => f.Finance.Amount(-5000, 5000, 2));

        while (true)
        {
            // Gera uma nova transação
            var transaction = faker.Generate();
            var message = $"{{\"userId\":\"{transaction.UserId}\", \"amount\":{transaction.Amount}}}";
            producer.Produce("transactions-topic", new Message<Null, string> { Value = message });

            Console.WriteLine($"Enviando transação: {message}");
            System.Threading.Thread.Sleep(1000); // 1 segundo entre transações
        }
    }
}
