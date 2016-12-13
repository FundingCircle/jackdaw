(ns kafka.streams.lambdas
  "Wrappers for the Java 'lambda' functions."
  (:import org.apache.kafka.streams.KeyValue
           [org.apache.kafka.streams.kstream Aggregator ForeachAction Initializer KeyValueMapper Predicate Reducer TransformerSupplier ValueJoiner ValueMapper]
           [org.apache.kafka.streams.processor ProcessorSupplier StreamPartitioner]))

(defn key-value
  "A key-value pair defined for a single Kafka Streams record."
  [[key value]]
  (KeyValue. key value))

(deftype FnAggregator [aggregator-fn]
  Aggregator
  (apply [this aggKey value aggregate]
    (aggregator-fn aggregate [aggKey value])))

(defn aggregator
  "Packages up a clojure fn in a kstream aggregator."
  ^Aggregator [aggregator-fn]
  (FnAggregator. aggregator-fn))

(deftype FnForeachAction [foreach-action-fn]
  ForeachAction
  (apply [this key value]
    (foreach-action-fn [key value])
    nil))

(defn foreach-action
  "Packages up a clojure fn in a kstream ForeachAction."
  [foreach-action-fn]
  (FnForeachAction. foreach-action-fn))

(deftype FnInitializer [initializer-fn]
  Initializer
  (apply [this]
    (initializer-fn)))

(defn initializer
  "Packages up a clojure fn in a kstream Initializer."
  ^Initializer [initializer-fn]
  (FnInitializer. initializer-fn))

(deftype FnKeyValueMapper [key-value-mapper-fn]
  KeyValueMapper
  (apply [this key value]
    (key-value (key-value-mapper-fn [key value]))))

(defn key-value-mapper
  "Packages up a clojure fn in a kstream key value mapper."
  [key-value-mapper-fn]
  (FnKeyValueMapper. key-value-mapper-fn))

(deftype FnSelectKeyValueMapper [select-key-value-mapper-fn]
  KeyValueMapper
  (apply [this key value]
    (select-key-value-mapper-fn [key value])))

(defn select-key-value-mapper
  "Packages up a clojure fn in a kstream key value mapper for use with
  `select-key`."
  [select-key-value-mapper-fn]
  (FnSelectKeyValueMapper. select-key-value-mapper-fn))

(deftype FnKeyValueFlatMapper [key-value-flatmapper-fn]
  KeyValueMapper
  (apply [this key value]
    (mapv key-value (key-value-flatmapper-fn [key value]))))

(defn key-value-flatmapper
  "Packages up a clojure fn in a kstream key value mapper for use with .flatMap.

  `key-value-flatmapper-fn` should be a function that takes a `[key value]` as a
  single parameter, and returns a list of `[key value]`."
  [key-value-flatmapper-fn]
  (FnKeyValueFlatMapper. key-value-flatmapper-fn))

(deftype FnPredicate [predicate-fn]
  Predicate
  (test [this key value]
    (boolean (predicate-fn [key value]))))

(defn predicate
  "Packages up a clojure fn in a kstream predicate."
  [predicate-fn]
  (FnPredicate. predicate-fn))

(deftype FnReducer [reducer-fn]
  Reducer
  (apply [this value1 value2]
    (reducer-fn value1 value2)))

(defn reducer
  "Packages up a clojure fn in a kstream reducer."
  ^Reducer [reducer-fn]
  (FnReducer. reducer-fn))

(deftype FnValueJoiner [value-joiner-fn]
  ValueJoiner
  (apply [this value1 value2]
    (value-joiner-fn value1 value2)))

(defn value-joiner
  "Packages up a clojure fn in a kstream value joiner."
  [value-joiner-fn]
  (FnValueJoiner. value-joiner-fn))

(deftype FnValueMapper [value-mapper-fn]
  ValueMapper
  (apply [this value]
    (value-mapper-fn value)))

(defn value-mapper
  "Packages up a clojure fn in a kstream value mapper."
  [value-mapper-fn]
  (FnValueMapper. value-mapper-fn))

(deftype FnStreamPartitioner [stream-partitioner-fn]
  StreamPartitioner
  (partition [this key val partition-count]
    (stream-partitioner-fn key val partition-count)))

(defn stream-partitioner
  "Packages up a clojure fn in a kstream partitioner."
  [stream-partitioner-fn]
  (when stream-partitioner-fn
    (FnStreamPartitioner. stream-partitioner-fn)))

(deftype FnProcessorSupplier [processor-supplier-fn]
  ProcessorSupplier
  (get [this]
    (processor-supplier-fn)))

(defn processor-supplier
  "Packages up a clojure fn in a kstream processor supplier."
  [processor-supplier-fn]
  (FnProcessorSupplier. processor-supplier-fn))

(deftype FnTransformerSupplier [transformer-supplier-fn]
  TransformerSupplier
  (get [this]
    (transformer-supplier-fn)))

(defn transformer-supplier
  "Packages up a clojure fn in a kstream transformer supplier."
  [processor-supplier-fn]
  (FnTransformerSupplier. processor-supplier-fn))
