using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.State;


public class WordCountProcessor
{

    const string BOOTSTRAP_SERVERS = "localhost:9092";

    const string APPLICATION_ID = "wordcount";

    const string INPUT_TOPIC = "input_text_topic";

    const string WORD_COUNT_OUTPUT_TOPIC = "word_count_output_topic";



    static async Task Main(string[] args) {
            var config = new StreamConfig<StringSerDes, StringSerDes>() {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = BOOTSTRAP_SERVERS,
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
            };

            StreamBuilder builder = new ();

            // Construct a `KStream` from the input topic "input_text_topic"
             IKStream<string, string> textLines = builder.Stream<string, string>(INPUT_TOPIC);

             IKTable<String, long> wordCounts = textLines
              // Split each text line, by whitespace after converting into lower case, into words. and use flatMapValues to map one sentence to multiple words
              
              .FlatMapValues(value => value.ToLower().Split(" "))
              // Group the split data by word so that we can subsequently count the occurrences per word.
              // This step re-keys (re-partitions) the input data, with the new record key being the words.
              // Note: No need to specify explicit serdes because the resulting key and value types
              // (String and String) match the application's default serdes.
              .GroupBy((k, w) => w)
              // Count the occurrences of each word (record key). use in memory state store else will look for rocksdb 
              .Count(InMemory.As<string, long>("test-store"));

            // Write the `KTable<String, Long>` to the output topic.
            wordCounts.ToStream()
                .MapValues((t) => t.ToString())
                .To(WORD_COUNT_OUTPUT_TOPIC);

            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                stream.Dispose();
            };

            await stream.StartAsync();

        }



}
