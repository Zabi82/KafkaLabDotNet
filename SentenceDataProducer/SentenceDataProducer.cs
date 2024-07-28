

using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;



public class SentenceDataProducer
{
    public static async Task Main(string[] args)
    {


        string brokerList = "localhost:9092";
        string topicName = "input_text_topic";

        string[] sentences = new String[]{"Kafka is a distributed middleware infrastructure", "Kafka has a producer and consumer API",
													"Confluent Kafka provides enterprise support and provides additional components",
													"Stream API internally uses Kafka client API for producer and consumer",
													"Kafka streaming support at least once and exactly once processing"};

        var config = new ProducerConfig { BootstrapServers = brokerList };

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {

            long counter = 1;
            Random rng = new Random();

            while(true) {

                int sentenceIndex = rng.Next(0, sentences.Length); 
                string sentence = sentences[sentenceIndex];

                try
                {
                    //aysnc produce
                    var deliveryReport = await producer.ProduceAsync(
                        topicName, new Message<string, string> { Key = counter.ToString(), Value = sentence });

                    Console.WriteLine($" key : {counter.ToString()}, value: {sentence} delivered to Topic: {deliveryReport.TopicPartitionOffset.Topic}, Partition: {deliveryReport.Partition},  Offset: {deliveryReport.Offset}  ");
                    counter++;
                    //sleep 2 sec
                    Thread.Sleep(2000);
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }


    }
}

