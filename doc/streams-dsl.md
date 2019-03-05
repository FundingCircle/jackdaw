# Kafka Streams DSL

## Rationale

The Jackdaw Streams API is a thin wrapper around the underlying Kafka Streams
DSL. It allows the definition of kafka streams applications using idiomatic
Clojure functions rather than the corresponding Java interop.

## Usage

If you've used the Java API, you'll be aware that the core operators are defined
as methods on the KStream and KTable classes. In Jackdaw, we expose these
methods as functions in the jackdaw.streams namespace with names that are
hyphenated versions of the corresponding Java method.

The [API
docs](https://cljdoc.org/d/fundingcircle/jackdaw/CURRENT/api/jackdaw.streams)
should be consulted for full details but the essential elements of a typical
streams app are described below

## Why?

Kafka Streams may be a good choice if you'd like to apply complex transformations
or aggregations to one or more data streams that can be made available as Kafka
topics, and you'd like to make the resulting output highly available.

If it's just a simple transformation you're after, you might consider
[SMT Transforms](https://docs.confluent.io/current/connect/transforms/index.html)
in combination with Kafka Connect.

### Topic Definition

```
(def input
  {:topic-name "foo"
   :replication-factor 1
   :partition-count 1
   :key-serde (Serdes/String)
   :value-serde (Serdes/String)})

(def output
  {:topic-name "bar"
   :replication-factor 1
   :partition-count 1
   :key-serde (Serdes/String)
   :value-serde (Serdes/Long)})

(def word-count-topics
  {:input input
   :output output})
```

### App Definition

```
(ns my.example.word-count
  (:require
    [jackdaw.streams :as k]))

(defn parse-line
  [line]
  (let [line (-> (.toLowerCase line (java.util.Locale/getDefault)))]
    (->> (.split line "\\W+")
         (into []))))

(defn word-count
  [in out]
  (fn [builder]
    (let [counts (-> (k/kstream builder in)
                     (k/flat-map-values (fn [line]
                                          (parse-line line)))
                     (k/group-by (fn [[k v]]
                                   v))
                     (k/count)
                     (k/to-kstream))]
      (k/to counts out)
      builder)))
```

### Start the Stream
```
(defn -main [& args]
  (let [builder (k/streams-builder)
        config (parse-args args)
        stream (k/kafka-streams (word-count input output) config)]
    (k/start stream)))
```

