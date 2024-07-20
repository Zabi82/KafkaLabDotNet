

using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;




public class HelloProducerCustomPartitioner
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
                    
                    TopicPartition topicPartition = new TopicPartition(topicName, new Partition(i%2 == 0 ? 0 : 1));

                    //aysnc produce
                    var deliveryReport = await producer.ProduceAsync(
                        topicPartition, new Message<string, string> { Key = key, Value = val });

                    Console.WriteLine($"key : {key}, value: {val} delivered to Topic: {deliveryReport.TopicPartitionOffset.Topic}, Partition: {deliveryReport.Partition},  Offset: {deliveryReport.Offset}  ");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }


    }

    
}

