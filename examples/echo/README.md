# Simple ECHO topology

This is a simple echo topology example.

## Install

Clofluent Platform 5.x 
https://www.confluent.io/download/

Clojure
https://clojure.org/guides/getting_started

## Running

### Setup
Remember to add Confluent's `bin` directory to your path (https://docs.confluent.io/current/quickstart/ce-quickstart.html#step-1-download-and-start-cp). 
Start Confluent Platform and create `input` and output `topics`. 

```bash
$ confluent start
$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic input
$ kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic output
```

### Run the topology

In the project directory `<repo-home>/jackdaw/examples/echo` run the topology.

```bash
$ clj -m echo-stream
Running! ctrl-c to stop!
```

### Produce Records

In another terminal, run the producer command and wait for prompt and type key and value separated by `:` ex `Foo:Bar`.

```bash
$ kafka-console-producer --broker-list localhost:9092 --property "parse.key=true" --property "key.separator=:" --topic input
> Foo:Bar
```

## Consume Records

To consume the echoed records you can close the producer `ctrl-c` or in a new terminal consume the `output` topic.

```bash
$ kafka-console-consumer --bootstrap-server localhost:9092 --property "print.key=true" --property "key.separator=:" --topic output --from-beginning
Foo:Bar
```
