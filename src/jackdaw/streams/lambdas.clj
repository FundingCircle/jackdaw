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
            Processor ProcessorSupplier ProcessorContext Record
            FixedKeyProcessor FixedKeyProcessorSupplier FixedKeyProcessorContext
            FixedKeyRecord]))

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
  ;; Kafka 4.x removed the single-partition `partition` method; `partitions`
  ;; returns an Optional set of target partitions. A nil result from the user fn
  ;; delegates to Kafka's default partitioner via Optional/empty.
  (partitions [_this topic-name key val partition-count]
    (if-let [p (stream-partitioner-fn topic-name key val partition-count)]
      (java.util.Optional/of #{(int p)})
      (java.util.Optional/empty))))

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

;; --- Transform/process adapters (Kafka 4.x) --------------------------------
;;
;; Kafka 4.x removed KStream.transform/transformValues in favour of
;; process/processValues, backed by the new Processor/FixedKeyProcessor API and
;; its ProcessorContext. The jackdaw transform* wrappers therefore require a
;; supplier fn that returns a Processor/FixedKeyProcessor - e.g. via the
;; transformer-with-ctx / value-transformer-with-ctx helpers below.
;;
;; The deprecated Transformer/ValueTransformer types are no longer accepted: they
;; can only be initialised with the legacy ProcessorContext, which the new API
;; cannot supply, so a context-dependent transformer would silently break.
;; Supplying one now fails fast with a clear error rather than being run without
;; a context.

(defn transform-supplier->processor-supplier
  "Adapts a jackdaw transform supplier fn to a Kafka ProcessorSupplier. The
  supplier fn must return an org.apache.kafka.streams.processor.api.Processor
  (e.g. via `transformer-with-ctx`)."
  ^ProcessorSupplier [supplier-fn]
  (reify ProcessorSupplier
    (get [_]
      (let [obj (supplier-fn)]
        (if (instance? Processor obj)
          obj
          (throw (ex-info (str "transform now requires an org.apache.kafka.streams.processor.api.Processor "
                               "(e.g. via jackdaw.streams.lambdas/transformer-with-ctx); the deprecated "
                               "Transformer type is not supported under Kafka 4.x.")
                          {:supplied (class obj)})))))))

(defn value-transform-supplier->fk-processor-supplier
  "Adapts a jackdaw transform-values supplier fn to a FixedKeyProcessorSupplier.
  The supplier fn must return an
  org.apache.kafka.streams.processor.api.FixedKeyProcessor (e.g. via
  `value-transformer-with-ctx`)."
  ^FixedKeyProcessorSupplier [supplier-fn]
  (reify FixedKeyProcessorSupplier
    (get [_]
      (let [obj (supplier-fn)]
        (if (instance? FixedKeyProcessor obj)
          obj
          (throw (ex-info (str "transform-values now requires an org.apache.kafka.streams.processor.api.FixedKeyProcessor "
                               "(e.g. via jackdaw.streams.lambdas/value-transformer-with-ctx); the deprecated "
                               "ValueTransformer type is not supported under Kafka 4.x.")
                          {:supplied (class obj)})))))))

(defn transformer-with-ctx
  "Helper to create a processor for use inside the jackdaw transform wrapper.
  Passed function should take three args - the context, key and value for the stream.
  The processor context allows access to stream internals such as state stores.
  The returned key-value is forwarded. E.g.
  ```
  (-> builder
      (k/stream topic)
      (k/transform
        (kl/transformer-with-ctx
          (fn [ctx k v]
            ...))))
  ```"
  [xfm-fn]
  (fn []
    (let [ctx (atom nil)]
      (reify Processor
        (init [_ context] (reset! ctx context))
        (process [_ record]
          (when-let [result (xfm-fn @ctx (.key ^Record record) (.value ^Record record))]
            (.forward ^ProcessorContext @ctx
                      ^Record (.withValue (.withKey ^Record record (.key ^KeyValue result))
                                          (.value ^KeyValue result)))))
        (close [_])))))

(defn value-transformer-with-ctx
  "Helper to create a fixed-key processor for use inside the jackdaw
  transform-values wrapper. Passed function should take two args - the context
  and value for the stream. The returned value is forwarded. E.g.
  ```
  (-> builder
      (k/stream topic)
      (k/transform-values
        (kl/value-transformer-with-ctx
          (fn [ctx v]
            ...))))
  ```"
  [xfm-fn]
  (fn []
    (let [ctx (atom nil)]
      (reify FixedKeyProcessor
        (init [_ context] (reset! ctx context))
        (process [_ record]
          (.forward ^FixedKeyProcessorContext @ctx
                    (.withValue ^FixedKeyRecord record (xfm-fn @ctx (.value ^FixedKeyRecord record)))))
        (close [_])))))
