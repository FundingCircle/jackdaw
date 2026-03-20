(ns jackdaw.streams.lambdas
  "Wrappers for the Java 'lambda' functions."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:import java.util.function.Function
           org.apache.kafka.streams.KeyValue
           [org.apache.kafka.streams.kstream
            Aggregator ForeachAction Initializer KeyValueMapper
            Merger Predicate Reducer
            ValueJoiner ValueMapper]
           [org.apache.kafka.streams.processor
            StreamPartitioner]
           [org.apache.kafka.streams.processor.api
            Processor ProcessorSupplier Record
            FixedKeyProcessor FixedKeyProcessorSupplier FixedKeyRecord]))

(set! *warn-on-reflection* true)

(defn key-value
  "A key-value pair defined for a single Kafka Streams record."
  [[key value]]
  (KeyValue. key value))

(deftype FnAggregator [aggregator-fn]
  Aggregator
  (apply [_this agg-key value aggregate]
    (aggregator-fn aggregate [agg-key value])))

(defn aggregator
  "Packages up a Clojure fn in a kstream aggregator."
  ^Aggregator [aggregator-fn]
  (FnAggregator. aggregator-fn))

(deftype FnForeachAction [foreach-action-fn]
  ForeachAction
  (apply [_this key value]
    (foreach-action-fn [key value])
    nil))

(defn foreach-action
  "Packages up a Clojure fn in a kstream ForeachAction."
  [foreach-action-fn]
  (FnForeachAction. foreach-action-fn))

(deftype FnInitializer [initializer-fn]
  Initializer
  (apply [_this]
    (initializer-fn)))

(defn initializer
  "Packages up a Clojure fn in a kstream Initializer."
  ^Initializer [initializer-fn]
  (FnInitializer. initializer-fn))

(deftype FnKeyValueMapper [key-value-mapper-fn]
  KeyValueMapper
  (apply [_this key value]
    (key-value (key-value-mapper-fn [key value]))))

(defn key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper."
  [key-value-mapper-fn]
  (FnKeyValueMapper. key-value-mapper-fn))

(deftype FnSelectKeyValueMapper [select-key-value-mapper-fn]
  KeyValueMapper
  (apply [_this key value]
    (select-key-value-mapper-fn [key value])))

(defn select-key-value-mapper
  "Packages up a Clojure fn in a kstream key value mapper for use with
  `select-key`."
  [select-key-value-mapper-fn]
  (FnSelectKeyValueMapper. select-key-value-mapper-fn))

(deftype FnKeyValueFlatMapper [key-value-flatmapper-fn]
  KeyValueMapper
  (apply [_this key value]
    (mapv key-value (key-value-flatmapper-fn [key value]))))

(defn key-value-flatmapper
  "Packages up a Clojure fn in a kstream key value mapper for use with .flatMap.

  `key-value-flatmapper-fn` should be a function that takes a `[key value]` as a
  single parameter, and returns a list of `[key value]`."
  [key-value-flatmapper-fn]
  (FnKeyValueFlatMapper. key-value-flatmapper-fn))

(deftype FnMerger [merger-fn]
  Merger
  (apply [_this agg-key aggregate1 aggregate2]
    (merger-fn agg-key aggregate1 aggregate2)))

(defn merger
  "Packages up a Clojure fn in a kstream merger (merges together two SessionWindows aggregate values)."
  ^Merger [merger-fn]
  (FnMerger. merger-fn))

(deftype FnPredicate [predicate-fn]
  Predicate
  (test [_this key value]
    (boolean (predicate-fn [key value]))))

(defn predicate
  "Packages up a Clojure fn in a kstream predicate."
  [predicate-fn]
  (FnPredicate. predicate-fn))

(deftype FnReducer [reducer-fn]
  Reducer
  (apply [_this value1 value2]
    (reducer-fn value1 value2)))

(defn reducer
  "Packages up a Clojure fn in a kstream reducer."
  ^Reducer [reducer-fn]
  (FnReducer. reducer-fn))

(deftype FnValueJoiner [value-joiner-fn]
  ValueJoiner
  (apply [_this value1 value2]
    (value-joiner-fn value1 value2)))

(deftype FnForeignKeyExtractor [foreign-key-extractor-fn]
  Function
  (apply [_this value]
    (foreign-key-extractor-fn value)))

(defn foreign-key-extractor
  "Packages up a Clojure fn into a Java Function - hopefully, redundant as of Clojure 1.12."
  [foreign-key-extractor-fn]
  (FnForeignKeyExtractor. foreign-key-extractor-fn))

(defn value-joiner
  "Packages up a Clojure fn in a kstream value joiner."
  [value-joiner-fn]
  (FnValueJoiner. value-joiner-fn))

(deftype FnValueMapper [value-mapper-fn]
  ValueMapper
  (apply [_this value]
    (value-mapper-fn value)))

(defn value-mapper
  "Packages up a Clojure fn in a kstream value mapper."
  [value-mapper-fn]
  (FnValueMapper. value-mapper-fn))

(deftype FnStreamPartitioner [stream-partitioner-fn]
  StreamPartitioner
  (partitions [_this topic-name key val partition-count]
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
  (process [_ record]
    (processor-fn @context (.key record) (.value record))))

(defn processor
  "Packages up a Clojure fn as a kstream processor."
  [processor-fn]
  (FnProcessor. (atom nil) processor-fn))

(deftype FnProcessorSupplier [processor-supplier-fn]
  ProcessorSupplier
  (get [_this]
    (processor processor-supplier-fn)))

(defn processor-supplier
  "Packages up a Clojure fn in a kstream processor supplier."
  ^ProcessorSupplier [processor-fn]
  (FnProcessorSupplier. processor-fn))

(deftype FnProcessorWithCtx [context xfm-fn]
  Processor
  (init [_this processor-context]
    (reset! context processor-context))
  (close [_this])
  (process [_this record]
    (let [ctx @context
          k (.key ^Record record)
          v (.value ^Record record)
          ts (.timestamp ^Record record)
          result (xfm-fn ctx k v)]
      (when result
        (.forward ^org.apache.kafka.streams.processor.api.ProcessorContext ctx
                  (Record. (.key ^KeyValue result)
                           (.value ^KeyValue result)
                           ts))))))

(defn processor-with-ctx
  "Helper to create a ProcessorSupplier for use with `k/process`.
  Replaces `transformer-with-ctx` for Kafka 4.0+, where `Transformer` has been removed.

  The passed function should take three args - the context, key and value for the stream.
  Return a `(key-value [k v])` to forward a record downstream, or nil to drop it.

  Example:
  ```
  (-> builder
      (k/with-kv-state-store {:store-name \"my-store\" ...})
      (k/kstream topic)
      (k/process
        (lambdas/processor-with-ctx
          (fn [ctx k v]
            (key-value [k (inc v)])))
        [\"my-store\"]))
  ```"
  ^ProcessorSupplier [xfm-fn]
  (reify ProcessorSupplier
    (get [_this] (FnProcessorWithCtx. (atom nil) xfm-fn))))


(deftype FnFlatProcessorWithCtx [context xfm-fn]
  Processor
  (init [_this processor-context]
    (reset! context processor-context))
  (close [_this])
  (process [_this record]
    (let [ctx @context
          k (.key ^Record record)
          v (.value ^Record record)
          ts (.timestamp ^Record record)
          results (xfm-fn ctx k v)]
      (doseq [kv results]
        (.forward ^org.apache.kafka.streams.processor.api.ProcessorContext ctx
                  (Record. (.key ^KeyValue kv)
                           (.value ^KeyValue kv)
                           ts))))))

(defn flat-processor-with-ctx
  "Helper to create a ProcessorSupplier for use with `k/process` that emits multiple records.
  Replaces flat-transform for Kafka 4.0+.
  The passed function should take three args [ctx k v] and return a seq of (key-value [k v]) pairs."
  ^ProcessorSupplier [xfm-fn]
  (reify ProcessorSupplier
    (get [_this] (FnFlatProcessorWithCtx. (atom nil) xfm-fn))))

(deftype FnValueProcessorWithCtx [context xfm-fn]
  FixedKeyProcessor
  (init [_this processor-context]
    (reset! context processor-context))
  (close [_this])
  (process [_this record]
    (let [ctx @context
          v (.value ^FixedKeyRecord record)
          result (xfm-fn ctx v)]
      (when result
        (.forward ^org.apache.kafka.streams.processor.api.FixedKeyProcessorContext ctx
                  (.withValue ^FixedKeyRecord record result))))))

(defn value-processor-with-ctx
  "Helper to create a FixedKeyProcessorSupplier for use with `k/process-values`.
  Replaces transform-values for Kafka 4.0+.
  The passed function should take two args [ctx v] and return a new value (or nil to drop).
  Key is preserved. Access topic via (.recordMetadata ctx) instead of (.topic ctx)."
  ^FixedKeyProcessorSupplier [xfm-fn]
  (reify FixedKeyProcessorSupplier
    (get [_this] (FnValueProcessorWithCtx. (atom nil) xfm-fn))))

(deftype FnFlatValueProcessorWithCtx [context xfm-fn]
  FixedKeyProcessor
  (init [_this processor-context]
    (reset! context processor-context))
  (close [_this])
  (process [_this record]
    (let [ctx @context
          v (.value ^FixedKeyRecord record)
          results (xfm-fn ctx v)]
      (doseq [result results]
        (.forward ^org.apache.kafka.streams.processor.api.FixedKeyProcessorContext ctx
                  (.withValue ^FixedKeyRecord record result))))))

(defn flat-value-processor-with-ctx
  "Helper to create a FixedKeyProcessorSupplier for use with `k/process-values` that emits multiple values.
  Replaces flat-transform-values for Kafka 4.0+.
  The passed function should take two args [ctx v] and return a seq of values (key preserved for each)."
  ^FixedKeyProcessorSupplier [xfm-fn]
  (reify FixedKeyProcessorSupplier
    (get [_this] (FnFlatValueProcessorWithCtx. (atom nil) xfm-fn))))
