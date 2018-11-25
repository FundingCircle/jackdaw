# Word Count

This tutorial contains a simple stream processing application using Jackdaw and Kafka Streams.

## Setting up

Before starting, it is recommended to install the Confluent Platform CLI which can be obtained from [https://www.confluent.io/download/](https://www.confluent.io/download/).

To install Clojure: [https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).

## Project structure

The project structure looks like this:
```
$ tree word-count
word-count
├── README.md
├── deps.edn
├── dev
│   └── system.clj
├── src
│   └── word_count.clj
└── test
    └── word_count_test.clj
```

The `deps.edn` file describes the project's dependencies and source paths.

The `system.clj` file contains functions to start, stop, and reset the app. These are required by the `user` namespace for interactive development and should not be invoked directly.

The `word_count.clj` file describes the topology. Word Count reads from a Kafka topic called 'input', logs the key and value, and writes the counts to a topic called 'output':

Pipe reads from a Kafka topic called "input", logs the key and value, and writes to a Kafka topic called "output". The topology uses a KTable to track how many times words are seen:
```
  (defn build-topology
    [builder]
    (let [text-input (-> (j/kstream builder (topic-config "input"))
                         (j/peek (fn [[k v]]
                                   (info (str {:key k :value v})))))

          count (-> text-input
                    (j/flat-map-values (fn [v]
                                         (str/split (str/lower-case v)
                                                    #"\W+")))
                    (j/group-by (fn [[_ v]] v)
                                (topic-config nil (Serdes/String)
                                              (Serdes/String)))
                    (j/count))]

      (-> count
          (j/to-kstream)
          (j/to (topic-config "output")))

      builder))
```

The `word_count_test.clj` file contains a test.

## Running the app

Let's get started! Fire up a Clojure REPL and load the `word-count` namespace. Then, start ZooKeeper and Kafka. If these services are already running, you may skip this step:
```
user> (confluent/start)
INFO zookeeper is up (confluent:288)
INFO kafka is up (confluent:288)
nil
```

Now, start the app.
```
user> (start)
INFO topic 'input' is created (jackdaw.admin.client:288)
INFO topic 'output' is created (jackdaw.admin.client:288)
INFO word-count is up (word-count:288)
{:app #object[org.apache.kafka.streams.KafkaStreams 0x72a2b1d3 "org.apache.kafka.streams.KafkaStreams@72a2b1d3"]}
```

The `user/start` function creates two Kafka topics needed by Word Count and starts it.

For the full list of topics:
```
user> (get-topics)
#{"word-count-KSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition"
  "output"
  "__confluent.support.metrics"
  "input"
  "word-count-KSTREAM-AGGREGATE-STATE-STORE-0000000004-changelog"}
```

With the app running, place a couple of records on the input stream:
```
(publish (topic-config "input") nil "all streams lead to kafka")
INFO {:key nil, :value "all streams lead to kafka"} (word-count:288)
nil
user> (publish (topic-config "input") nil "hello kafka streams")
INFO {:key nil, :value "hello kafka streams"} (word-count:288)
nil
```

Word Count logs the key and value to the standard output.

To read from the output stream:
```
user> (get-keyvals (topic-config "output"))
(("all" 1)
 ("streams" 1)
 ("lead" 1)
 ("to" 1)
 ("kafka" 1)
 ("hello" 1)
 ("kafka" 2)
 ("streams" 2))
```

This concludes this tutorial.

## Interactive development

For interactive development, reload the file and invoke `user/reset`. These stops the app, deletes topics and internal state using a regex, and recreates the topics and restarts the app. The details are in the `system` namespace.

## Running tests

To run tests, load the `word-count-test` namespace and invoke a test runner using your editor, or from the command line:
```
clj -Atest
```
