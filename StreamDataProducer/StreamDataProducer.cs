

using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Runtime.CompilerServices;



public class StreamDataProducer
{
    public static async Task Main(string[] args)
    {


        string brokerList = "localhost:9092";
        string topicName = "daily_temperature_celsius";

        var config = new ProducerConfig { BootstrapServers = brokerList };

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {

            long counter = 1;
            Random rng = new Random();

            while(true) {
               
                int temp = rng.Next(25, 35); 


           

                try
                {
                    //aysnc produce
                    var deliveryReport = await producer.ProduceAsync(
                        topicName, new Message<string, string> { Key = counter.ToString(), Value = temp.ToString() });

                    Console.WriteLine($" key : {counter.ToString()}, value: {temp.ToString()} delivered to Topic: {deliveryReport.TopicPartitionOffset.Topic}, Partition: {deliveryReport.Partition},  Offset: {deliveryReport.Offset}  ");
                    counter++;
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }


    }
}

