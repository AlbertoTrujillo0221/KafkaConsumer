using Confluent.Kafka;
using Newtonsoft.Json;

var config = new ConsumerConfig
{
    GroupId = "LiteTestValue-consumer-group",
    BootstrapServers = "localhost:9092",
    AutoOffsetReset = AutoOffsetReset.Earliest,
};

using var consumer = new ConsumerBuilder<Null, string>(config).Build();

consumer.Subscribe("LiteTestValue");

CancellationTokenSource token = new();

try
{
    while (true)
    {
        var response = consumer.Consume(token.Token);
        if(response.Message != null)
        {
            var result = JsonConvert.DeserializeObject<Order>
                (response.Message.Value);
            Console.WriteLine($"OrderNumber: {result.OrderNumber}, Price: {result.Price}," +
                $"City: {result.City}");
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine(ex.ToString());
}

public record Order(string OrderNumber, int Price, string City);