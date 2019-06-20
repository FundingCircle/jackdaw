# Jackdaw Streams API

## Rationale

The Jackdaw Streams API is a thin wrapper around the underlying Kafka Streams
DSL. It allows the definition of streaming applications using idiomatic
Clojure functions rather than the corresponding Java interop.

Kafka Streams may be a good choice if you'd like to apply complex transformations
or aggregations to one or more data streams that can be made available as Kafka
topics, and you'd like to make the resulting output highly available.

If it's just a simple transformation you're after, you might consider
[SMT Transforms](https://docs.confluent.io/current/connect/transforms/index.html)
in combination with Kafka Connect.


## Usage

If you've used the Java API, you'll be aware that the core operators are defined
as methods on the KStream and KTable classes. In Jackdaw, we expose these
methods as functions in the `jackdaw.streams` namespace with names that are
hyphenated versions of the corresponding Java method.

The [API
docs](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.streams)
should be consulted for full details but the essential elements of a typical
streams app are described below


### Topic Definition

```
(def topic-metadata

  {:input
   {:topic-name "input"
    :partition-count 1
    :replication-factor 1
    :key-serde (jackdaw.serdes.edn/serde)
    :value-serde (jackdaw.serdes.edn/serde)}

   :output
   {:topic-name "output"
    :partition-count 1
    :replication-factor 1
    :key-serde (jackdaw.serdes.edn/serde)
    :value-serde (jackdaw.serdes.edn/serde)}})
```


### App Definition

```
(ns my.example.word-count
  (:require
    [clojure.string :as str]
    [jackdaw.streams :as j]))

(defn split-lines
  [input-string]
  (str/split (str/lower-case input-string) #"\W+"))

(defn topology-builder
  [topic-metadata]
  (fn [builder]
    (let [text-input (j/kstream builder (:input topic-metadata))

          counts (-> text-input
                     (j/flat-map-values split-lines)
                     (j/group-by (fn [[_ v]] v))
                     (j/count))]

      (-> counts
          (j/to-kstream)
          (j/to (:output topic-metadata)))

      builder)))
```


### Start the App

```
(defn -main
  [& args]
  (let [app-config (parse-args args)
        builder (j/streams-builder)
        topology ((topology-builder topic-metadata) builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    app))
```
