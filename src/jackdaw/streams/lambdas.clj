(ns jackdaw.streams.lambdas
  "Wrappers for the Java 'lambda' functions."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:import org.apache.kafka.streams.KeyValue
           [org.apache.kafka.streams.kstream
            Aggregator ForeachAction Initializer KeyValueMapper
            Merger Predicate Reducer Transformer TransformerSupplier
            ValueJoiner ValueMapper ValueTransformer
            ValueTransformerSupplier]
           [org.apache.kafka.streams.processor
            Processor ProcessorSupplier StreamPartitioner]))

(set! *warn-on-reflection* true)

(defn key-value
  "A key-value pair defined for a single Kafka Streams record."
  [[key value]]
  (KeyValue. key value))

(deftype FnAggregator [aggregator-fn]
  Aggregator
  (apply [this agg-key value aggregate]
    (aggregator-fn aggregate [agg-key value])))

(defn aggregator
  "Packages up a Clojure fn in a kstream aggregator."
  ^Aggregator [aggregator-fn]
  (FnAggregator. aggregator-fn))

(deftype FnForeachAction [foreach-action-fn]
  ForeachAction
  (apply [this key value]
    (foreach-action-fn [key value])
    nil))

(defn foreach-action
  "Packages up a Clojure fn in a kstream ForeachAction."
  [foreach-action-fn]
  (FnForeachAction. foreach-action-fn))

(deftype FnInitializer [initializer-fn]
  Initializer
  (apply [this]
    (initializer-fn)))

(defn initializer
  "Packages up a Clojure fn in a kstream Initializer."
  ^Initializer [initializer-fn]
  (FnInitializer. initializer-fn))

(deftype FnKeyValueMapper [key-value-mapper-fn]
  KeyValueMapper
  (apply [this key value]
    (key-value (key-value-mapper-fn [key value]))))

(defn key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper."
  [key-value-mapper-fn]
  (FnKeyValueMapper. key-value-mapper-fn))

(deftype FnSelectKeyValueMapper [select-key-value-mapper-fn]
  KeyValueMapper
  (apply [this key value]
    (select-key-value-mapper-fn [key value])))

(defn select-key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper for use with
  `select-key`."
  [select-key-value-mapper-fn]
  (FnSelectKeyValueMapper. select-key-value-mapper-fn))

(deftype FnKeyValueFlatMapper [key-value-flatmapper-fn]
  KeyValueMapper
  (apply [this key value]
    (mapv key-value (key-value-flatmapper-fn [key value]))))

(defn key-value-flatmapper
  "Packages up a Clojure fn in a kstream key value mapper for use with .flatMap.

  `key-value-flatmapper-fn` should be a function that takes a `[key value]` as a
  single parameter, and returns a list of `[key value]`."
  [key-value-flatmapper-fn]
  (FnKeyValueFlatMapper. key-value-flatmapper-fn))

(deftype FnMerger [merger-fn]
  Merger
  (apply [this agg-key aggregate1 aggregate2]
    (merger-fn agg-key aggregate1 aggregate2)))

(defn merger
  "Packages up a Clojure fn in a kstream merger (merges together two SessionWindows aggregate values)."
  ^Merger [merger-fn]
  (FnMerger. merger-fn))

(deftype FnPredicate [predicate-fn]
  Predicate
  (test [this key value]
    (boolean (predicate-fn [key value]))))

(defn predicate
  "Packages up a Clojure fn in a kstream predicate."
  [predicate-fn]
  (FnPredicate. predicate-fn))

(deftype FnReducer [reducer-fn]
  Reducer
  (apply [this value1 value2]
    (reducer-fn value1 value2)))

(defn reducer
  "Packages up a Clojure fn in a kstream reducer."
  ^Reducer [reducer-fn]
  (FnReducer. reducer-fn))

(deftype FnValueJoiner [value-joiner-fn]
  ValueJoiner
  (apply [this value1 value2]
    (value-joiner-fn value1 value2)))

(defn value-joiner
  "Packages up a Clojure fn in a kstream value joiner."
  [value-joiner-fn]
  (FnValueJoiner. value-joiner-fn))

(deftype FnValueMapper [value-mapper-fn]
  ValueMapper
  (apply [this value]
    (value-mapper-fn value)))

(defn value-mapper
  "Packages up a Clojure fn in a kstream value mapper."
  [value-mapper-fn]
  (FnValueMapper. value-mapper-fn))

(deftype FnStreamPartitioner [stream-partitioner-fn]
  StreamPartitioner
  (partition [this topic-name key val partition-count]
    (stream-partitioner-fn topic-name key val partition-count)))

(defn stream-partitioner
  "Packages up a Clojure fn in a kstream partitioner."
  [stream-partitioner-fn]
  (when stream-partitioner-fn
    (FnStreamPartitioner. stream-partitioner-fn)))

(deftype FnProcessor [context processor-fn]
  Processor
  (close [_])
  (init [_ processor-context]
    (reset! context processor-context))
  (process [_ key message]
    (processor-fn @context key message)))

(defn processor
  "Packages up a Clojure fn as a kstream processor."
  [processor-fn]
  (FnProcessor. (atom nil) processor-fn))

(deftype FnProcessorSupplier [processor-supplier-fn]
  ProcessorSupplier
  (get [this]
    (processor processor-supplier-fn)))

(defn processor-supplier
  "Packages up a Clojure fn in a kstream processor supplier."
  [processor-fn]
  (FnProcessorSupplier. processor-fn))

(deftype FnTransformerSupplier [transformer-supplier-fn]
  TransformerSupplier
  (get [this]
    (transformer-supplier-fn)))

(defn transformer-supplier
  "Packages up a Clojure fn in a kstream transformer supplier."
  [transformer-supplier-fn]
  (FnTransformerSupplier. transformer-supplier-fn))

(deftype FnValueTransformerSupplier [value-transformer-supplier-fn]
  ValueTransformerSupplier
  (get [this]
    (value-transformer-supplier-fn)))

(defn value-transformer-supplier
  "Packages up a Clojure fn in a kstream value transformer supplier."
  [value-transformer-supplier-fn]
  (FnValueTransformerSupplier. value-transformer-supplier-fn))

(defn transformer-with-ctx
  "Packages a Clojure fn inside the structure required by the transformer
  supplier, and records the ProcessorContext for it - passing the captured
  context to the provided function."
  [xfm]
  (fn []
    (let [ctx (atom nil)]
      (reify Transformer
        (init [_ processor-context]
          (reset! ctx processor-context))
        (close [_])
        (transform [_ k v]
          (xfm @ctx k v))))))

(defn value-transformer-with-ctx
  "Packages a Clojure fn inside the structure required by the value transformer
  supplier, and records the ProcessorContext for it - passing the captured
  context to the provided function."
  [xfm]
  (fn []
    (let [ctx (atom nil)]
      (reify ValueTransformer
        (init [_ processor-context]
          (reset! ctx processor-context))
        (close [_])
        (transform [_ v]
          (xfm @ctx v))))))

(defn with-stores
  "Decorates a call to a fn used in a transform with the named state stores
  from the Processor context. The named stores are passed in a map, keyed by
  the store name. The name may be a keyword or string.
  Primarily this is an aid to testing, as it allows the wrapped fn to access
  state stores without having to know about the ProcessorContext."
  [store-names xfm]
  (fn [ctx & args]
    (let [state-stores (into {} (map (fn [n]
                                       [n (.getStateStore ctx (name n))])) store-names)]
      (apply xfm ctx state-stores args))))
