
using System.Text;
using RabbitMQ.Client;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client.Events;

namespace RabbitMQ;

public class DirectExchange
{
    public static void TestRun()
    {
        var factory = new ConnectionFactory()
        {
            Uri = new Uri("amqp://guest:guest@localhost:15672/"),
        };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var exchangeName = "direct.exchange";

        channel.ExchangeDeclare(exchangeName, ExchangeType.Direct, arguments: null);

        //ucl
        {
            var queueName = "ucl";
            var routeKeyName = "user.created";
            channel.QueueDeclare(queueName,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: null);

            channel.QueueBind(queueName, exchangeName, routeKeyName);
            channel.BasicQos(0, 1, false);
        }

        //uul
        {
            var queueName = "uul";
            var routeKeyName = "user.updated";
            channel.QueueDeclare(queueName,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: null);

            channel.QueueBind(queueName, exchangeName, routeKeyName);
            channel.BasicQos(0, 1, false);
        }

        //ucl.two
        {
            var queueName = "ucl.two";
            var routeKeyName = "user.created";
            channel.QueueDeclare(queueName,
                        durable: true,
                        exclusive: false,
                        autoDelete: true,
                        arguments: null);

            channel.QueueBind(queueName, exchangeName, routeKeyName);
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

            channel.BasicConsume("ucl", true, consumer);
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

            channel.BasicConsume("uul", true, consumer);
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

            channel.BasicConsume("ucl.two", true, consumer);
        }
        //consumer 4
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Consumer 4 : {message}");
            };

            channel.BasicConsume("ucl", true, consumer);
        }

        //Msg:1
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 1", userName = "syed" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(exchangeName, "user.created", null, body);
        }
        //Msg:2
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 2", userName = "sirojul" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(exchangeName, "user.created", null, body);
        }
        //Msg:3
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 3", userName = "islam" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(exchangeName, "user.created", null, body);
        }
        //Msg:4
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 4", userName = "anik", old = "shed" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(exchangeName, "user.updated", null, body);
        }
        //Msg:5
        {
            var message = JsonSerializer.Serialize(new { msg = "msg 5", userName = "islam" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish("", "ucl.two", null, body);
        }

        // Console.WriteLine("Enter to exit");
        Console.ReadLine();
    }
}
