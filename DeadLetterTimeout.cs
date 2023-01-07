
using System.Text;
using RabbitMQ.Client;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client.Events;

namespace RabbitMQ;

public class DeadLetterTimeout
{
    public static void TestRun()
    {
        var factory = new ConnectionFactory()
        {
            Uri = new Uri("amqp://guest:guest@localhost:15672/"),
        };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var name = "deadletter.timeout";
        var workerExchange = $"ex.{name}";
        var workerQueue = $"q.{name}";

        var dlxExchange = $"ex.dlx.{name}";
        var dlxQueue = $"q.dlx.{name}";

        //dlxExchange
        channel.ExchangeDeclare(dlxExchange, ExchangeType.Fanout);
        channel.QueueDeclare(dlxQueue,
         durable: true,
         exclusive: false,
         autoDelete: false,
         arguments: null);

        channel.QueueBind(dlxQueue, dlxExchange, string.Empty);
        channel.BasicQos(0, 1, false);

        //workerExchange
        {
            channel.ExchangeDeclare(workerExchange, ExchangeType.Fanout);

            var queueArgs = new Dictionary<string, object> {
                        { "x-dead-letter-exchange", dlxExchange },
                        { "x-message-ttl", 20000 },
                    };
            channel.QueueDeclare(workerQueue,
             durable: true,
             exclusive: false,
             autoDelete: false,
             arguments: queueArgs);


            channel.QueueBind(workerQueue, workerExchange, string.Empty);
            channel.BasicQos(0, 1, false);
        }

        //consumer : without consummer


        //massage for success
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 1", userName = "suhut" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(workerExchange, string.Empty, null, body);
        }
        //massage for fail
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 2", userName = "wadiyo" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(workerExchange, string.Empty, null, body);
        }


        // Console.WriteLine("Enter to exit");
        Console.ReadLine();
    }
}
