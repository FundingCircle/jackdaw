# Pipe

This tutorial contains a simple stream processing application using Jackdaw and Kafka Streams.

Before starting, it is recommended to install the Confluent Platform CLI which can be obtained from `https://www.confluent.io/download/`. To install Clojure: `https://clojure.org/guides/getting_started`.

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

The `pipe.clj` file describes the topology. Pipe reads from a Kafka topic called 'input', logs the key and value, and writes to a Kafka topic called 'output':
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

Let's get started! Fire up a Clojure REPL and load the `pipe` namespace. Then, start ZooKeeper and Kafka. If these services are already running, you may skip this step:
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
INFO pipe is up (pipe:288)
{:app #object[org.apache.kafka.streams.KafkaStreams 0x225dcbb9 "org.apache.kafka.streams.KafkaStreams@225dcbb9"]}
```

The `user/start` function creates two Kafka topics needed by Pipe and starts it.

For the full list of topics:
```
user> (get-topics)
#{"output" "__confluent.support.metrics" "input"}
```

With the app running, place a new record on the input stream:
```
user> (publish (topic-config "input") nil "this is a pipe")
INFO {:key nil, :value "this is a pipe"} (pipe:288)
nil
```
Pipe logs the key and value to the standard output.

To read from the output stream:
```
user> (get-keyvals (topic-config "output"))
((nil "this is a pipe"))
```

This concludes this tutorial.

For interactive development, reload the file and invoke `user/reset`. These stops the app, deletes topics and internal state using a regex, and recreates the topics and restarts the app.

To run tests, load the `pipe-test` namespace and invoke a test runner using your editor, or from the command line:
```
clj -Atest
```
