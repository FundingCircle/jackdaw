# Jackdaw Client API

## Rationale

The Jackdaw Client API wraps the core Kafka `Producer`<sup>[1](#producerapi)</sup> and
`Consumer`<sup>[2](#consumerapi)</sup> APIs and provides functions for building or
unpacking some of the supporting objects like Callbacks, Serdes, ConsumerRecords etc.

Higher level concepts in the kafka ecosystem like Kafka Streams, Kafka Connect, and KSQL all
build on these core APIs so acquiring a deep understanding will be rewarded with increased
understanding of the many associated technologies.

While Kafka's surface API is quite small, the functionality it provides is deep. You
can get up and running very quickly with a simple example but to fully understand it's
capabilities there is no substitute for reading the upstream documentation. The
scope of this guide is therefore limited to demonstrating how to use API via Jackdaw
and connecting the reader to the relevant parts of the upstream documentation for
further reading.

## Producing

The producer example below demonstrates how to use the Kafka Producer API. The configuration<sup>[3](#producerconfig)</sup>
is represented as a simple map (Jackdaw will convert this to a `Properties` object) and in
this example, the producer is minimally configured just to illustrate a few
important options.

 * "bootstrap.servers=localhost:9092" tells the producer to establish a connection with
   the kafka broker running on the default port at localhost

 * "client.id=foo" means that the string 'foo' will be used in all requests to brokers so
   that they can be distinguished by more than just host and IP. It will also form part of
   name of the metrics reported by both brokers and the producing application itself

 * "acks=all" means that the leader will wait for the full set of in-sync replicas to
   acknowledge the result and complete the response. This is the slowest but most durable
   setting. The default is '1' which means that the leader will respond as soon as the record
   has been written to it's own log. This allows faster throughput at the cost of reduced
   durability.

Producers are usually created using the `with-open` macro so that they are automatically
closed either when evaluation reaches the end of the body or an exception is thrown. By
default, the StringSerializer is used to serialize the key and value provided for inclusion
in the ProducerRecord that is submitted to the leader and

Within the body, the `jc/produce!` function is used to request a write to the specified
Kafka topic. This function returns a delay immediately which can be `deref`'d to wait
for the result of the Kafka `.send` call which includes metadata like the timestamp
and offset of the written record.

The [KafkaProducer javadocs](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)
provide more detailed information about how the producer works behind the scenes.


```
(ns producer-example
  (:require
    [jackdaw.client :as jc]))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "acks" "all"
   "client.id" "foo"})

(with-open [my-producer (jc/producer producer-config)]
  @(jc/produce! my-producer "foo" "1" "hi mom!"))
```

## Consuming

The consumer example below demonstrates how to use the Kafka Consumer API. The configuration<sup>[5](#consumerconfig)</sup>
is represented as a simple map (Jackdaw will convert this to a `Properties` object), and in
this example, the Consumer is minimally configured just to illustrate a few important options

 * "bootstrap.servers=localhost:9092" tells the consumer to establish a connection with
   the kafka broker running on the default port at localhost

 * "group.id=foo" means that this consumer is part of the 'foo' consumer group. Other consumers
   with the same id form a pool of consumers that share the workload providing scalability and
   fault tolerance

Consumers are usually created using the `with-open` macro so that they are automatically
closed either when evaluation reaches the end of the body or an exception is thrown. By default
the StringDeserializer is used to deserialize the key and value before being made available
in the ConsumerRecord. In this example, the consumer will see all records written to the "foo"
topic due to the use of `jc/subscribe`.

The `jackdaw.client.log/log` function takes a consumer instance that has already been subscribed
to one or more topics, and returns a lazy infinite sequence of "datafied" records in the order
that they were received by calls to the Consumer's `.poll` method. In this example, we just
write the record to standard out to demonstrate the keys that are available in each record. To
see what other keys are available, see data/consumer.clj<sup>[6](#consumerdata)</sup>

The [KafkaConsumer javadocs](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
provide more detailed information about how the consumer works behind the scenes.

```
(ns consumer-example
  (:require
    [jackdaw.client :as jc]
    [jackdaw.client.log :as jl])
  (:import
    (org.apache.kafka.common.serialization Serdes)))

(def consumer-config
  {"bootstrap.servers" "localhost:9092"
   "group.id"  "com.foo.my-consumer"})

(def topic-foo
  {:topic-name "foo"})

(with-open [my-consumer (-> (jc/consumer consumer-config)
                            (jc/subscribe [topic-foo]))]
  (doseq [{:keys [key value partition timestamp offset]} (jl/log my-consumer)]
    (println "key: " key)
    (println "value: " value)
    (println "partition: " partition)
    (println "timestamp: " timestamp)
    (println "offset: " offset)))
```

## References

 <a name="producerapi">1</a>: https://kafka.apache.org/documentation/#producerapi <br />
 <a name="consumerapi">2</a>: https://kafka.apache.org/documentation/#consumerapi <br />
 <a name="producerconfig">3</a>: https://kafka.apache.org/documentation/#producerconfigs <br />
 <a name="serdesdirectory">4</a>: https://github.com/FundingCircle/jackdaw/blob/master/src/jackdaw/serdes <br />
 <a name="consumerconfig">5</a>: https://kafka.apache.org/documentation/#consumerconfigs <br />
 <a name="consumerdata">6</a>: https://github.com/FundingCircle/jackdaw/blob/master/src/jackdaw/data/consumer.clj
