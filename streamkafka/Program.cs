using Confluent.Kafka;
using System;
using Microsoft.Extensions.Configuration;

class StreamKafka
{
    static void Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.WriteLine("Please provide the configuration file path as a command line argument");
        }

        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile(args[0])
            .Build();

        const string topic = "purchases";

        string[] users = { "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" };
        string[] items = { "book", "alarm clock", "t-shirts", "gift card", "batteries" };

        using (var producer = new ProducerBuilder<string, string>(
            configuration.AsEnumerable()).Build())
        {
            var numProduced = 0;
            const int numMessages = 100;
            for (int i = 0; i < numMessages; ++i)
            {
                Random rnd = new Random();
                // var user = users[rnd.Next(users.Length)];
                var item = items[rnd.Next(items.Length)];

                // producer.Produce(topic, new Message<string, string> { Key = user, Value = item },
                Partition partition = 0;
                TopicPartition topicPartition = new TopicPartition(topic, partition);

                producer.Produce(topicPartition, new Message<string, string> { Value = item },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                        }
                        else
                        {
                            // Console.WriteLine($"Produced event to topic {topic}: key = {user,-10} value = {item}");
                            Console.WriteLine($"Produced event to topic {topic}: value = {item}");
                            numProduced += 1;
                        }
                    });
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
        }
    }
}