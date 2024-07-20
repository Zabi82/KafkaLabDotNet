

using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Runtime.CompilerServices;



public class HelloProducer
{
    public static async Task Main(string[] args)
    {


        string brokerList = "localhost:9092";
        string topicName = "hello_world_topic";

        var config = new ProducerConfig { BootstrapServers = brokerList };

        using (var producer = new ProducerBuilder<string, string>(config).Build())
        {



            for (int i = 0; i < 10; i++) {
                string key =  "Key" + i;
                string val = "Value" + i;

           

                try
                {
                    //aysnc produce
                    var deliveryReport = await producer.ProduceAsync(
                        topicName, new Message<string, string> { Key = key, Value = val });

                    Console.WriteLine($" key : {key}, value: {val} delivered to Topic: {deliveryReport.TopicPartitionOffset.Topic}, Partition: {deliveryReport.Partition},  Offset: {deliveryReport.Offset}  ");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }


    }
}

