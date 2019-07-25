(ns jackdaw.streams.interop
  "Clojure wrapper to kafka streams."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [count map reduce group-by merge filter peek])
  (:require [jackdaw.streams.protocols :refer :all]
            [jackdaw.streams.lambdas :refer :all])
  (:import [java.util
            Collection]
           [java.util.regex
            Pattern]
           [org.apache.kafka.common.serialization
            Serde]
           [org.apache.kafka.streams
            KafkaStreams]
           [org.apache.kafka.streams
            StreamsBuilder]
           [org.apache.kafka.streams.kstream
            Aggregator Consumed GlobalKTable Initializer Joined
            JoinWindows KGroupedStream KGroupedTable KStream KTable
            KeyValueMapper Materialized Merger Predicate Printed Produced
            Reducer Serialized SessionWindowedKStream SessionWindows
            TimeWindowedKStream ValueJoiner ValueMapper
            ValueMapperWithKey ValueTransformerSupplier Windows]
           [org.apache.kafka.streams.processor
            StreamPartitioner]))

(set! *warn-on-reflection* true)

(defn topic->consumed [{:keys [key-serde value-serde]}]
  (Consumed/with key-serde value-serde))

(defn topic->produced [{:keys [key-serde value-serde partition-fn]}]
  (if partition-fn
    (Produced/with key-serde value-serde (->FnStreamPartitioner partition-fn))
    (Produced/with key-serde value-serde)))

(defn topic->serialized [{:keys [key-serde value-serde]}]
  (Serialized/with key-serde value-serde))

(defn topic->materialized [{:keys [topic-name key-serde value-serde]}]
  (cond-> (Materialized/as ^String topic-name)
    key-serde (.withKeySerde key-serde)
    value-serde (.withValueSerde value-serde)))

(declare clj-kstream clj-ktable clj-kgroupedtable clj-kgroupedstream
         clj-global-ktable clj-session-windowed-kstream
         clj-time-windowed-kstream)

(def ^:private kstream-memo
  "Returns a kstream for the topic, creating a new one if needed."
  (memoize
   (fn [streams-builder {:keys [topic-name] :as topic-config}]
     (clj-kstream
      (.stream ^StreamsBuilder streams-builder
               ^String topic-name
               ^Consumed (topic->consumed topic-config))))))

(def ^:private kstream-memo-patterned
  "Returns a kstream for the topic, creating a new one if needed."
  (memoize
   (fn [streams-builder topic-config topic-pattern]
     (clj-kstream
      (.stream ^StreamsBuilder streams-builder
               ^Pattern topic-pattern
               ^Consumed (topic->consumed topic-config))))))

(def ^:private ktable-memo
  "Returns a ktable for the topic, creating a new one if needed."
  (memoize
   (fn [streams-builder {:keys [topic-name] :as topic-config}
        store-name]
     (clj-ktable
      (.table ^StreamsBuilder streams-builder
              ^String topic-name
              ^Consumed (topic->consumed topic-config)
              ^Materialized (topic->materialized topic-config))))))

(deftype CljStreamsBuilder [^StreamsBuilder streams-builder]
  IStreamsBuilder

  (kstream
    [_ topic-config]
    (kstream-memo streams-builder topic-config))

  (kstream
    [_ topic-config topic-pattern]
    (kstream-memo-patterned streams-builder topic-config topic-pattern))

  (kstreams
    [_ topic-configs]
    (clj-kstream
     (let [topic-names (clojure.core/map :topic-name topic-configs)]
       (.stream streams-builder
                ^Collection topic-names
                ;; Assume all the topics use the same serdes.
                ^Consumed (topic->consumed (first topic-configs))))))

  (ktable
    [_ {:keys [topic-name] :as topic-config}]
    (ktable-memo streams-builder topic-config topic-name))

  (ktable
    [_ topic-config store-name]
    (ktable-memo streams-builder topic-config store-name))

  (global-ktable [_ {:keys [topic-name] :as topic-config}]
    (clj-global-ktable
     (.globalTable ^StreamsBuilder streams-builder
                   ^String topic-name
                   ^Consumed (topic->consumed topic-config))))

  (streams-builder*
    [_]
    streams-builder))

(defn streams-builder
  "Makes a streams builder."
  []
  (CljStreamsBuilder. (StreamsBuilder.)))

(deftype CljKStream [^KStream kstream]
  IKStreamBase
  (left-join
    [_ ktable value-joiner-fn]
    (clj-kstream
     (.leftJoin ^KStream kstream
                ^KTable (ktable* ktable)
                ^ValueJoiner (value-joiner value-joiner-fn))))

  (left-join
    [_ ktable value-joiner-fn
     {key-serde :key-serde this-value-serde :value-serde}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.leftJoin kstream
                ^KTable (ktable* ktable)
                ^ValueJoiner (value-joiner value-joiner-fn)
                (Joined/with key-serde this-value-serde other-value-serde))))

  (peek
    [_ peek-fn]
    (clj-kstream
     (.peek kstream ^ForeachAction (foreach-action peek-fn))))

  (filter
    [_ predicate-fn]
    (clj-kstream
     (.filter kstream ^Predicate (predicate predicate-fn))))

  (filter-not
    [_ predicate-fn]
    (clj-kstream
     (.filterNot kstream ^Predicate (predicate predicate-fn))))

  (group-by
    [_ key-value-mapper-fn]
    (clj-kgroupedstream
     (.groupBy kstream ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn))))

  (group-by
    [_ key-value-mapper-fn topic-config]
    (clj-kgroupedstream
     (.groupBy kstream
               ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn)
               ^Serialized (topic->serialized topic-config))))

  (map-values
    [_ value-mapper-fn]
    (clj-kstream
     (.mapValues kstream ^ValueMapper (value-mapper value-mapper-fn))))

  IKStream
  (branch
    [_ predicate-fns]
    (mapv clj-kstream
          (->> (into-array Predicate (mapv predicate predicate-fns))
               (.branch kstream))))

  (flat-map
    [_ key-value-mapper-fn]
    (clj-kstream
     (.flatMap kstream ^KeyValueMapper (key-value-flatmapper key-value-mapper-fn))))

  (for-each!
    [_ foreach-fn]
    (.foreach kstream ^ForeachAction (foreach-action foreach-fn))
    nil)

  (print!
    [_]
    (.print kstream (Printed/toSysOut))
    nil)

  (through
    [_ {:keys [topic-name] :as topic-config}]
    (clj-kstream
     (.through kstream topic-name ^Produced (topic->produced topic-config))))

  (to!
    [_ {:keys [topic-name] :as topic-config}]
    (.to kstream ^String topic-name ^Produced (topic->produced topic-config))
    nil)

  (flat-map-values
    [_ value-mapper-fn]
    (clj-kstream
     (.flatMapValues kstream ^ValueMapper (value-mapper value-mapper-fn))))

  (group-by-key
    [_]
    (clj-kgroupedstream
     (.groupByKey kstream)))

  (group-by-key
    [_ topic-config]
    (clj-kgroupedstream
     (.groupByKey ^KStream kstream
                  ^Serialized (topic->serialized topic-config))))

  (join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.join ^KStream kstream
            ^KStream (kstream* other-kstream)
            ^ValueJoiner (value-joiner value-joiner-fn)
            ^JoinWindows windows)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :key-serde this-value-serde :value-serde}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.join kstream
            ^KStream (kstream* other-kstream)
            ^ValueJoiner (value-joiner value-joiner-fn)
            ^JoinWindows windows
            (Joined/with key-serde this-value-serde other-value-serde))))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.leftJoin ^KStream kstream
                ^KStream (kstream* other-kstream)
                ^ValueJoiner (value-joiner value-joiner-fn)
                ^JoinWindows windows)))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows
     {:keys [key-serde value-serde]}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.leftJoin kstream
                ^KStream (kstream* other-kstream)
                ^ValueJoiner (value-joiner value-joiner-fn)
                ^JoinWindows windows
                (Joined/with key-serde value-serde other-value-serde))))

  (map
    [_ key-value-mapper-fn]
    (clj-kstream
     (.map kstream ^KeyValueMapper (key-value-mapper key-value-mapper-fn))))

  (merge
    [_ other-kstream]
    (clj-kstream
      (.merge kstream
              ^KStream (kstream* other-kstream))))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.outerJoin ^KStream kstream
                 ^KStream (kstream* other-kstream)
                 ^ValueJoiner (value-joiner value-joiner-fn)
                 ^JoinWindows windows)))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :key-serde value-serde :value-serde}
     {other-value-serde :value-serde}]
    (clj-kstream
     (.outerJoin ^KStream kstream
                 ^KStream (kstream* other-kstream)
                 ^ValueJoiner (value-joiner value-joiner-fn)
                 ^JoinWindows windows
                 (Joined/with key-serde value-serde other-value-serde))))

  (process!
    [_ processor-supplier-fn state-store-names]
    (.process ^KStream kstream
              ^ProcessorSupplier (processor-supplier processor-supplier-fn)
              (into-array String state-store-names)))

  (select-key
    [_ select-key-value-mapper-fn]
    (clj-kstream
     (.selectKey ^KStream kstream
                 ^KeyValueMapper (select-key-value-mapper select-key-value-mapper-fn))))

  (transform
    [this transformer-supplier-fn]
    (transform this transformer-supplier-fn []))

  (transform
    [_ transformer-supplier-fn state-store-names]
    (clj-kstream
     (.transform ^KStream kstream
                 ^TransformerSupplier (transformer-supplier transformer-supplier-fn)
                 ^"[Ljava.lang.String;" (into-array String state-store-names))))

  (transform-values
    [this value-transformer-supplier-fn]
    (transform-values this value-transformer-supplier-fn []))

  (transform-values
    [_ value-transformer-supplier-fn state-store-names]
    (clj-kstream
     (.transformValues ^KStream kstream
                       ^ValueTransformerSupplier (value-transformer-supplier value-transformer-supplier-fn)
                       ^"[Ljava.lang.String;" (into-array String state-store-names))))

  (join-global
    [_ global-ktable key-value-mapper-fn joiner-fn]
    (clj-kstream
     (.join kstream
            ^GlobalKTable (global-ktable* global-ktable)
            ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn)
            ^ValueJoiner (value-joiner joiner-fn))))

  (left-join-global
    [_ global-ktable key-value-mapper-fn joiner-fn]
    (clj-kstream
     (.leftJoin kstream
                ^GlobalKTable (global-ktable* global-ktable)
                ^KeyValueMapper (select-key-value-mapper key-value-mapper-fn)
                ^ValueJoiner (value-joiner joiner-fn))))

  (kstream* [_]
    kstream))

(defn clj-kstream
  "Makes a CljKStream object."
  [kstream]
  (CljKStream. kstream))

(deftype CljKTable [^KTable ktable]
  IKStreamBase
  (left-join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.leftJoin ^KTable ktable
                ^KTable (ktable* other-ktable)
                ^ValueJoiner (value-joiner value-joiner-fn))))

  (filter
    [_ predicate-fn]
    (clj-ktable
     (.filter ^KTable ktable
              ^Predicate (predicate predicate-fn))))

  (filter-not
    [_ predicate-fn]
    (clj-ktable
     (.filterNot ^KTable ktable
                 ^Predicate (predicate predicate-fn))))

  (map-values
    [_ value-mapper-fn]
    (clj-ktable
     (.mapValues ktable ^ValueMapper (value-mapper value-mapper-fn))))

  IKTable
  (group-by
    [_ key-value-mapper-fn]
    (clj-kgroupedtable
     (.groupBy ktable ^KeyValueMapper (key-value-mapper key-value-mapper-fn))))

  (group-by
    [_ key-value-mapper-fn topic-config]
    (clj-kgroupedtable
     (.groupBy ktable
               ^KeyValueMapper (key-value-mapper key-value-mapper-fn)
               ^Serialized (topic->serialized topic-config))))

  (join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.join ^KTable ktable
            ^KTable (ktable* other-ktable)
            ^ValueJoiner (value-joiner value-joiner-fn))))

  (outer-join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.outerJoin ^KTable ktable
                 ^KTable (ktable* other-ktable)
                 ^ValueJoiner (value-joiner value-joiner-fn))))

  (to-kstream
    [_]
    (clj-kstream
     (.toStream ^KTable ktable)))

  (to-kstream
    [_ key-value-mapper-fn]
    (clj-kstream
     (.toStream ^KTable ktable
                ^KeyValueMapper (key-value-mapper key-value-mapper-fn))))

  (ktable* [_]
    ktable))

(defn clj-ktable
  "Makes a CljKTable object."
  [ktable]
  (CljKTable. ktable))

(deftype CljGlobalKTable [^GlobalKTable global-ktable]
  IGlobalKTable

  (global-ktable* [_]
    global-ktable))

(defn clj-global-ktable
  "Makes a CljKTable object."
  [global-ktable]
  (CljGlobalKTable. global-ktable))

(deftype CljKGroupedTable [^KGroupedTable kgroupedtable]
  IKGroupedBase
  (aggregate
    [_ initializer-fn adder-fn subtractor-fn
     {:keys [topic-name value-serde]}]
    (clj-ktable
     (.aggregate ^KGroupedTable kgroupedtable
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator adder-fn)
                 ^Aggregator (aggregator subtractor-fn)
                 (doto (Materialized/as ^String topic-name) (.withValueSerde value-serde)))))

  (aggregate
    [_ initializer-fn adder-fn subtractor-fn]
    (clj-ktable
     (.aggregate ^KGroupedTable kgroupedtable
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator adder-fn)
                 ^Aggregator (aggregator subtractor-fn))))

  (count
    [_]
    (clj-ktable
     (.count ^KGroupedTable kgroupedtable)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^KGroupedTable kgroupedtable
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ adder-fn subtractor-fn topic-config]
    (clj-ktable
     (.reduce ^KGroupedTable kgroupedtable
              ^Reducer (reducer adder-fn)
              ^Reducer (reducer subtractor-fn)
              ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ adder-fn subtractor-fn]
    (clj-ktable
     (.reduce ^KGroupedTable kgroupedtable
              ^Reducer (reducer adder-fn)
              ^Reducer (reducer subtractor-fn))))

  IKGroupedTable
  (kgroupedtable*
    [_]
    kgroupedtable))

(defn clj-kgroupedtable
  "Makes a CljKGroupedTable object."
  [kgroupedtable]
  (CljKGroupedTable. kgroupedtable))

(deftype CljKGroupedStream [^KGroupedStream kgroupedstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn {:keys [topic-name value-serde]}]
    (clj-ktable
     (.aggregate ^KGroupedStream kgroupedstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn)
                 (doto (Materialized/as ^String topic-name) (.withValueSerde value-serde)))))

  (aggregate
    [_ initializer-fn aggregator-fn]
    (clj-ktable
     (.aggregate ^KGroupedStream kgroupedstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn))))

  (count
    [_]
    (clj-ktable
     (.count ^KGroupedStream kgroupedstream)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^KGroupedStream kgroupedstream
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn topic-config]
    (clj-ktable
     (.reduce ^KGroupedStream kgroupedstream
              ^Reducer (reducer reducer-fn)
              ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn]
    (clj-ktable
     (.reduce ^KGroupedStream kgroupedstream
              ^Reducer (reducer reducer-fn))))

  IKGroupedStream
  (windowed-by-time
    [_ windows]
    (clj-time-windowed-kstream
     (.windowedBy ^KGroupedStream kgroupedstream ^Windows windows)))

  (windowed-by-session
    [_ windows]
    (clj-session-windowed-kstream
     (.windowedBy ^KGroupedStream kgroupedstream ^SessionWindows windows)))

  (kgroupedstream*
    [_]
    kgroupedstream))

(defn clj-kgroupedstream
  "Makes a CljKGroupedStream object."
  [kgroupedstream]
  (CljKGroupedStream. kgroupedstream))

(deftype CljTimeWindowedKStream [^TimeWindowedKStream windowed-kstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn {:keys [topic-name value-serde]}]
    (clj-ktable
     (.aggregate ^TimeWindowedKStream windowed-kstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn)
                 (doto (Materialized/as ^String topic-name) (.withValueSerde value-serde)))))

  (count
    [_]
    (clj-ktable
     (.count ^TimeWindowedKStream windowed-kstream)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^TimeWindowedKStream windowed-kstream
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn topic-config]
    (clj-ktable
     (.reduce ^TimeWindowedKStream windowed-kstream
              ^Reducer (reducer reducer-fn)
              ^Materialized (topic->materialized topic-config))))

  ITimeWindowedKStream
  (time-windowed-kstream*
    [_]
    windowed-kstream))

(defn clj-time-windowed-kstream
  "Makes a CljTimeWindowedKStream object."
  [windowed-kstream]
  (CljTimeWindowedKStream. windowed-kstream))

(deftype CljSessionWindowedKStream [^SessionWindowedKStream windowed-kstream]
  IKGroupedBase
  (aggregate
    [_ initializer-fn aggregator-fn merger-fn {:keys [topic-name value-serde]}]
    (clj-ktable
     (.aggregate ^SessionWindowedKStream windowed-kstream
                 ^Initializer (initializer initializer-fn)
                 ^Aggregator (aggregator aggregator-fn)
                 ^Merger (merger merger-fn)
                 (doto (Materialized/as ^String topic-name) (.withValueSerde value-serde)))))

  (count
    [_]
    (clj-ktable
     (.count ^SessionWindowedKStream windowed-kstream)))

  (count
    [_ topic-config]
    (clj-ktable
     (.count ^SessionWindowedKStream windowed-kstream
             ^Materialized (topic->materialized topic-config))))

  (reduce
    [_ reducer-fn topic-config]
    (clj-ktable
     (.reduce ^SessionWindowedKStream windowed-kstream
              ^Reducer (reducer reducer-fn)
              ^Materialized (topic->materialized topic-config))))

  ISessionWindowedKStream
  (session-windowed-kstream*
    [_]
    windowed-kstream))

(defn clj-session-windowed-kstream
  "Makes a CljSessionWindowedKStream object."
  [windowed-kstream]
  (CljSessionWindowedKStream. windowed-kstream))
