using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;


public class StreamProcessor
{

    const string BOOTSTRAP_SERVERS = "localhost:9092";

    const string APPLICATION_ID = "streams-exercise";

    const string CELSIUS_TOPIC = "daily_temperature_celsius";

    const string FARENHEIT_TOPIC = "daily_temperature_farenheit";

    const string HOT_TOPIC = "hot_days";

    static async Task Main(string[] args) {
            var config = new StreamConfig<StringSerDes, StringSerDes>() {
            ApplicationId = APPLICATION_ID,
            BootstrapServers = BOOTSTRAP_SERVERS,
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest
            };
            
            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, string>(CELSIUS_TOPIC)
                                .Map((k, v) => KeyValuePair.Create(k, getFarenheit(v)))
                                .To(FARENHEIT_TOPIC);
                                                  

            builder.Stream<string, string>(FARENHEIT_TOPIC)
            .Map((k, v) => KeyValuePair.Create(v, v))
            .Filter((k,v) => Convert.ToDouble(v) > 87.8)
            .To(HOT_TOPIC);                    
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                stream.Dispose();
            };

            await stream.StartAsync();

        }

        private static string getFarenheit(string celsius) {
            double fraction = 9.0 / 5.0;
            double result = Math.Round((Int32.Parse(celsius) * fraction + 32),2);
            return result.ToString();
        }

}
