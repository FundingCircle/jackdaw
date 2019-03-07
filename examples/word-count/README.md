# Word Count

This is the classic 'word count' example done as a stream processing
application using the Jackdaw Streams API.


## Setting up

Before starting, it is recommended to downloand and install the
Confluent CLI which can be obtained from
[https://www.confluent.io/download/](https://www.confluent.io/download/).

To install Clojure:
[https://clojure.org/guides/getting_started](https://clojure.org/guides/getting_started).


## Project structure

The project structure looks like this:
```
tree
.
├── deps.edn
├── dev
│   └── system.clj
├── README.md
├── resources
│   ├── logback.xml
│   └── metamorphosis.txt
├── src
│   └── word_count.clj
└── test
    └── word_count_test.clj

4 directories, 7 files
```

The `deps.edn` file describes the project's source paths and
dependencies.

The `system.clj` file contains functions to start and stop the
app. These are used by the `user` namespace for interactive
development.

The `word_count.clj` file describes the app and the topology. The
application reads from a Kafka topic called `input` and splits the
input value into words. It puts the count on a Kafka topic called
`output` for each word seen:
```
(defn topology-builder
  [topic-metadata]
  (fn [builder]
    (let [text-input (-> (j/kstream builder (:input (topic-metadata)))
                         (j/peek (fn [[k v]] (info (str {:key k :value v})))))

          counts (-> text-input
                     (j/flat-map-values split-lines)
                     (j/group-by (fn [[_ v]] v))
                     (j/count))]

      (-> counts
          (j/to-kstream)
          (j/to (:output (topic-metadata))))

      builder)))
```

The `word_count_test.clj` file contains a test


## Running the app

Let's get started! Start Confluent Platform using the Confluent CLI
`start` command.
```
<path-to-confluent>/bin/confluent start
```

Then change to the Word Count project directory and start a REPL.
```
cd <path-to-jackdaw>/examples/word-count
clj
```

You should see output like the following:
```
Clojure 1.10.0
user=>
```

Enter the following at the `user=>` prompt:
```
(reset)
```

If you see output like the following, congratulations, the app is
running!
```
23:01:25.939 [main] INFO  system - internal state is deleted
23:01:26.093 [main] INFO  word-count - word-count is up
{:app #object[org.apache.kafka.streams.KafkaStreams 0xb8b2184 "org.apache.kafka.streams.KafkaStreams@b8b2184"]}
```

Let's put a couple of records on the input topic:
```
(publish (:input (topic-metadata)) nil "all streams lead to kafka")
(publish (:input (topic-metadata)) nil "hello kafka streams")
```

We can also get the result:
```
(get-keyvals (:output (topic-metadata)))
```

You should see output like the following:
```
(["all" 1]
 ["streams" 1]
 ["lead" 1]
 ["to" 1]
 ["kafka" 1]
 ["hello" 1]
 ["kafka" 2]
 ["streams" 2])
```

For more a more in depth walkthrough, see the comment block in the
`word_count.clj` file.


## Running tests

To run tests, load the `word-count-test` namespace in your editor and
invoke a test runner, or from the command line:
```
clj -Atest
```
