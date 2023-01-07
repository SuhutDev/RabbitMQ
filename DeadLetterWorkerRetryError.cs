
using System.Text;
using RabbitMQ.Client;
using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client.Events;

namespace RabbitMQ;

public class DeadLetterWorkerRetryError
{
    public static void TestRun()
    {
        var factory = new ConnectionFactory()
        {
            Uri = new Uri("amqp://guest:guest@localhost:15672/"),
        };
        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        var name = "deadletter-worker-retry-error";

        var workerExchange = $"ex.{name}";
        var workerQueue = $"q.{name}";

        var retryExchange = $"ex.retry.{name}";
        var retryQueue = $"q.retry.{name}";
        var retryCountMax = 3;

        var errorExchange = $"ex.error.{name}";
        var errorQueue = $"q.error.{name}";

        //ErrorExchange
        {
            channel.ExchangeDeclare(errorExchange, ExchangeType.Fanout);
            channel.QueueDeclare(errorQueue,
             durable: true,
             exclusive: false,
             autoDelete: false,
             arguments: null);

            channel.QueueBind(errorQueue, errorExchange, string.Empty);
            channel.BasicQos(0, 1, false);
        }

        //RetryExchange 
        {
            channel.ExchangeDeclare(retryExchange, ExchangeType.Fanout);
            var queueArgs = new Dictionary<string, object> {
                        { "x-dead-letter-exchange", workerExchange },
                        { "x-message-ttl", 4000 },
                    };

            channel.QueueDeclare(retryQueue,
             durable: true,
             exclusive: false,
             autoDelete: false,
             arguments: queueArgs);

            channel.QueueBind(retryQueue, retryExchange, string.Empty);
            channel.BasicQos(0, 1, false);
        }

        //WorkerExchange
        {
            channel.ExchangeDeclare(workerExchange, ExchangeType.Fanout);
            var queueArgs = new Dictionary<string, object> {
                        { "x-dead-letter-exchange", retryExchange },
                    };
            channel.QueueDeclare(workerQueue,
             durable: true,
             exclusive: false,
             autoDelete: false,
             arguments: queueArgs);

            channel.QueueBind(workerQueue, workerExchange, string.Empty);
            channel.BasicQos(0, 1, false);
        }


        //WorkerQueue 
        {
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                if (message.Contains("success"))
                {
                    channel.BasicAck(e.DeliveryTag, false);
                }
                else
                {
                    long retryCount = 0;
                    if (e.BasicProperties.Headers is null)
                    {

                    }
                    else
                    {
                        var originalQueueHeader = (byte[])e.BasicProperties.Headers["x-first-death-queue"];
                        var originalQueue = Encoding.UTF8.GetString(originalQueueHeader);
                        if (originalQueue == workerQueue)
                        {
                            if (e.BasicProperties.Headers.ContainsKey("x-death"))
                            {
                                var deathProperties = (List<object>)e.BasicProperties.Headers["x-death"];
                                var lastRetry = (Dictionary<string, object>)deathProperties[0];
                                var count = lastRetry["count"];
                                retryCount = (long)count;
                            }
                        }

                    }

                    if (retryCount >= retryCountMax)
                    {
                        var properties = channel.CreateBasicProperties();
                        properties.Headers = e.BasicProperties.Headers;

                        //ErrorExchange
                        channel.BasicPublish(errorExchange, string.Empty, properties, e.Body);
                        channel.BasicAck(e.DeliveryTag, false);
                    }
                    else
                    {
                        channel.BasicNack(e.DeliveryTag, false, false);
                    }
                }
            };

            channel.BasicConsume(workerQueue, false, consumer);
        }

        //massage for success
        {
            var message = JsonSerializer.Serialize(new { msg = "success", userName = "suhut" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(workerExchange, string.Empty, null, body);
        }
        //massage for fail
        {
            var message = JsonSerializer.Serialize(new { msg = "fail", userName = "wadiyo" });
            var body = Encoding.UTF8.GetBytes(message);
            Console.WriteLine($"Publish : {message}");
            channel.BasicPublish(workerExchange, string.Empty, null, body);
        }


        // Console.WriteLine("Enter to exit");
        Console.ReadLine();
    }
}
