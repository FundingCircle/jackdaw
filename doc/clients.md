# Jackdaw Client API

## Rationale

The Jackdaw Client API wraps the core Kafka `Producer`<sup>[1](#producerapi)</sup> and
`Consumer`<sup>[2](#consumerapi)</sup> APIs and provides functions for building or
unpacking some of the supporting objects like Callbacks, Serdes, ConsumerRecords etc.

## Producing

The producer example below demonstrates how to use the Kafka Producer API. The configuration<sup>[3](#producerconfig)</sup>
is represented as a simple map (Jackdaw will convert this to a `Properties` object), and the
key and value serializers are specified when creating the producer to override the default
(which would be the `StringSerializer`).

Producers are usually created using the `with-open` macro so that they are automatically
closed either when evaluation reaches the end of the body or an exception is thrown.

Within the body, the `jc/produce!` function is used to request a write to the specified
Kafka topic. This function returns a delay immediately which can be `deref`'d to wait
for the result of the Kafka `.send` call which includes metadata like the timestamp
and offset of the written record.

In this example, the JSON serde converts the message from a plain old Clojure map into a
JSON byte-array while building the ProducerRecord that is eventually passed to the
KafkaProducer's `.send` method. Other Serdes are available<sup>[4](#serdesdirectory)</sup>

```
(ns producer-example
  (:require
    [jackdaw.serdes.json :as json]
    [jackdaw.client :as jc])
  (:import
    (org.apache.kafka.common.serialization Serdes)))

(def producer-config
  {"bootstrap.servers" "localhost:9092"
   "acks" "all"
   "client.id" "com.foo.my-producer"})

(with-open [my-producer (jc/producer producer-config (Serdes/IntegerSerde) (json/serde))]
  (jc/produce! my-producer "foo" 1 {:id 1, :payload "hi mom!"}))
```

## Consuming

The consumer example below demonstrates how to use the Kafka Consumer API. The configuration<sup>[5](#consumerconfig)</sup>
is represented as a simple map (Jackdaw will convert this to a `Properties` object), and the
key and value serializers are specified when creating the consumer to override the default
(which would be the `StringSerializer`).

Consumers are usually created using the `with-open` macro so that they are automatically
closed either when evaluation reaches the end of the body or an exception is thrown. In this
example, we subscribe to the "foo" topic immediately after creating the consumer using
`jc/subscribe`.

The `jackdaw.client.log/log` function takes a consumer instance that has already been subscribed
to one or more topics, and returns a lazy infinite sequence of "datafied" records in the order
that they were received by calls to the Consumer's `.poll` method. In this example, we just
write the record to standard out to demonstrate the keys that are available in each record. To
see what other keys are available, see data/consumer.clj<sup>[6](#consumerdata)</sup>

```
(ns consumer-example
  (:require
    [jackdaw.serdes.json :as json]
    [jackdaw.client :as jc]
    [jackdaw.client.log :as jl])
  (:import
    (org.apache.kafka.common.serialization Serdes)))

(def consumer-config
  {"bootstrap.servers" "localhost:9092"
   "group.id"  "com.foo.my-consumer"})

(def topic-foo
  {:topic-name "foo"})

(with-open [my-consumer (-> (jc/consumer consumer-config (Serdes/IntegerSerde) (json/serde))
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
