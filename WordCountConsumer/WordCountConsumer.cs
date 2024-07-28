

using System;
using System.Linq;
using System.Threading;
using Confluent.Kafka;



public class WordCountConsumer
{

    public static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "word_test",
            EnableAutoCommit = true,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,


        };



        var topics = "word_count_output_topic";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };



        using (var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .Build())
        {
            consumer.Subscribe(topics);

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine($"Received message from Topic: {consumeResult.TopicPartitionOffset.Topic} , Partition: {consumeResult.TopicPartition.Partition}, Offset: {consumeResult.Offset} , Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}");

                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                    
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                consumer.Close();
            }
        }

        Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

    }
}
