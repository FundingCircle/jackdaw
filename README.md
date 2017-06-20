# kstreams-common

A thin clojure wrapper around the kafka streams API.

# Usage

Some common scenarios and function signatures are described here. If you don't see
an example of the API you'd like to use, check the tests. If there isn't one there,
please raise an issue.

```
(ns kafka-streams-demo
  (:require
    [kafka.streams :as k]
    [kafka.streams.interop :as interop]
    [kafka.serdes :as serde]))

(defn demo-topic [topic-name]
  {:topic.metadata/name topic-name
   :kafka.serdes/key-serde (serde/serde :kafka.serde/edn)
   :kafka.serdes/value-serde (serde/serde :kafka.serde/edn)})
```

### kstreams/ktables

```
(let [table-of-foos (k/ktable (demo-topic "foo"))
      stream-of-bars (k/kstream (demo-topic "bar"))]
  ...)
```

### transform stream values

Transform the values in a stream while keeping the keys in-tact

```
(defn transformer [s]
  (k/map-values (fn [v]
                  (transformed v))))
```

### transform keys *and* values

Sometimes you need to also update the key

```
(defn transformer [s]
  (k/map (fn [[k v]]
           [(transformed-key k)
            (transformed-value v)])))
```




