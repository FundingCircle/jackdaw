# Pipe

This tutorial contains a simple stream processing application using Jackdaw and Kafka Streams.

## Setting up

Before starting, it is recommended to install the Confluent Platform CLI which can be obtained from [https://www.confluent.io/download/](https://www.confluent.io/download/).

To install Clojure: [https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).

## Project structure

The project structure looks like this:
```
$ tree pipe
pipe
├── README.md
├── deps.edn
├── dev
│   └── system.clj
├── src
│   └── pipe.clj
└── test
    └── pipe_test.clj
```

The `deps.edn` file describes the project's dependencies and source paths.

The `system.clj` file contains functions to start, stop, and reset the app. These are required by the `user` namespace for interactive development and should not be invoked directly.

The `pipe.clj` file describes the app and topology. Pipe reads from a Kafka topic called "input", logs the key and value, and writes to a Kafka topic called "output":
```
(defn build-topology
  [builder]
  (-> (j/kstream builder (topic-config "input"))
      (j/peek (fn [[k v]]
                (info (str {:key k :value v}))))
      (j/to (topic-config "output")))
  builder)
```

The `pipe_test.clj` file contains a test.

## Running the app

Let's get started! Fire up a Clojure REPL, then, start ZooKeeper and Kafka. 
You can use the helper `docker-compose.yml` file provided with this project, i.e. run
```
docker-compose up -d
```

Now, start the app, i.e. in the REPL run:
```
user> (start)
INFO topic 'input' is created (jackdaw.admin.client:288)
INFO topic 'output' is created (jackdaw.admin.client:288)
INFO pipe is up (pipe:288)
{:app #object[org.apache.kafka.streams.KafkaStreams 0x225dcbb9 "org.apache.kafka.streams.KafkaStreams@225dcbb9"]}
```

The `start` function creates two Kafka topics needed by Pipe and starts it.

For the full list of topics, type:
```
user> (list-topics)
#{"output" "__confluent.support.metrics" "input"}
```

With the app running, place a new record on the input stream:
```
user> (publish {:k1 "Some value" :k2 "Some other value"})
{:topic-name "input",
 :partition 0,
 :timestamp 1620320890747,
 :offset 0,
 :serialized-key-size -1,
 :serialized-value-size 42}
```

Pipe logs the key and value to the standard output.

To read from the output stream:
```
user> (consume)
({:k1 "Some value", :k2 "Some other value"})
```

This concludes this tutorial.

## Interactive development

For interactive development, reload the file and invoke `user/reset`. These stops the app, deletes topics and internal state using a regex, and recreates the topics and restarts the app. The details are in the `system` namespace.

## Running tests

To run tests, load the `pipe-test` namespace and invoke a test runner using your editor, or from the command line:
```
clj -Atest
```
