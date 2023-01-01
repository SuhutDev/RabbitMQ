
using System.Text;
using RabbitMQ.Client;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client.Events;

namespace RabbitMQ;

public class HeadersExchange
{
    public static void TestRun()
    {
        var factory = new ConnectionFactory()
        {
            Uri = new Uri("amqp://guest:guest@localhost:15672/"),
        };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var exchangeName = "headers.exchange";

        channel.ExchangeDeclare(exchangeName, ExchangeType.Headers, arguments: null);

        //ucl
        {
            var headers = new Dictionary<string, object> { { "x-match", "any" }, { "country", "us" }, { "city", "cd" } };

            var queueName = "ucl";
            channel.QueueDeclare(queueName,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: null);


            channel.QueueBind(queueName, exchangeName, string.Empty, headers);
            channel.BasicQos(0, 1, false);
        }

        //ucl.two
        {
            var queueName = "ucl.two";
            channel.QueueDeclare(queueName,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: null);

            var headers = new Dictionary<string, object> { { "x-match", "all" }, { "country", "bd" }, { "city", "cd" } };

            channel.QueueBind(queueName, exchangeName, string.Empty, headers);
            channel.BasicQos(0, 1, false);
        }

        //consumer 1
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Consumer 1 : {message}");
            };


            channel.BasicConsume(queue: "ucl", autoAck: true, consumer: consumer);
        }
        //consumer 2
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Consumer 2 : {message}");
            };


            channel.BasicConsume(queue: "ucl", autoAck: true, consumer: consumer);
        }
        //consumer 3
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Consumer 3 : {message}");
            };

            channel.BasicConsume(queue: "ucl.two", autoAck: true, consumer: consumer);

        }


        //Msg:1
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 1", userName = "syed" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");

            var properties = channel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object> { { "country", "us" }, { "city", "ab" } };

            channel.BasicPublish(exchangeName, string.Empty, properties, body);
        }
        //Msg:2
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 2", userName = "sirojul" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");

            var properties = channel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object> { { "country", "us" }, { "city", "cd" } };

            channel.BasicPublish(exchangeName, string.Empty, properties, body);
        }
        //Msg:3
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 3", userName = "islam" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");

            var properties = channel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object> { { "country", "uk" }, { "city", "ab" } };

            channel.BasicPublish(exchangeName, string.Empty, properties, body);
        }
        //Msg:4
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 4", userName = "anik", old = "shed" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");

            var properties = channel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object> { { "country", "bd" }, { "city", "cd" } };

            channel.BasicPublish(exchangeName, string.Empty, properties, body);
        }


        // Console.WriteLine("Enter to exit");
        Console.ReadLine();
    }
}
