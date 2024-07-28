# KafkaLabDotNet
This repository has .Net examples for some of the exercises in the repo https://github.com/Zabi82/KafkaLab which are in Java

### Simple Kafka Producer & Consumer using .Net

Download / clone this project

Import the project into VSCode

Create a new topic "hello_world_topic" with 2 partitions and replication factor 1

```

./kafka-topics --bootstrap-server localhost:9092 --create --topic hello_world_topic --partitions 2 --replication-factor 1

```

Write a producer and consumer programs to produce and consume from the above topic. Refer to the classes namely HelloProducer and HelloConsumer 

Try to run the consumer with different consumer group id and observe that the new consumer gets a complete copy of the messages

Stop the consumer and run the producer once again to inject new messages in the topic. Before starting the consumer again run below command
to check the offsets for each partition and the lag for the consumer

```

./kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group test-group

```

Now run the consumer again and recheck the above command and notice that the lag is now 0 in both partitions
as all messages are consumed


### Understanding different producer and consumer configuration options

1) seeking to beginning offset, end offset or specific offsets - Refer HelloConsumerSeekOffset
2) committing offsets manually using commitySync and commitAsync methods - Refer HelloConsumerManualCommit
3) Custom Partitioner in producer - Refer HelloProducerCustomPartitioner
4) Re-balancing Listener - Refer HelloConsumerRebalanceHandler - Observe how partitions are assigned and revoked as part of rebalancing when new consumer joins a consumer group or an existing consumer leaves. Need to run multiple instances of the consumer to observe this

### Kafka Streaming Example - Converting/Filtering Weather Data

Create the following topics with 1 partition and replication factor 1

```
daily_temperature_celsius
daily_temperature_farenheit
hot_days
```

Generate random weather data to a topic daily_temperature_celsius
(Run StreamDataProducer)

Using Kafka Streams, convert the celsius value to farenheit and write results to a new topic daily_temperature_farenheit 

Also filter all temperatures greater than 31 degree celsius and write to new topic hot_days

Refer to the solution in StreamProcessor class

Write Consumers for the output topics and inspect the data or stream the output topics and print results. Refer TempFarenheitConsumer and HotDaysConsumer


### Kafka Streaming Example - Word Count Example
Create the following topics with 1 partition and replication factor 1

```
input_text_topic
word_count_output_topic
```

Run the SentenceDataProducer to produce some sentence every few seconds. WordCountProcessor streams this and performs a word count and outputs the results

Write Consumers for the output topics and inspect the data or stream the output topics and print results. Refer WordCountConsumer
