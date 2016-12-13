(ns kafka.streams.interop
  "Clojure wrapper to kafka streams."
  (:refer-clojure :exclude [count map reduce group-by merge filter])
  (:require [kafka.streams.lambdas :refer :all]
            [kafka.streams :refer :all])
  (:import org.apache.kafka.common.serialization.Serde
           org.apache.kafka.streams.KafkaStreams
           [org.apache.kafka.streams.kstream KGroupedTable KStream KStreamBuilder KTable Predicate Windows]
           org.apache.kafka.streams.processor.TopologyBuilder))

(set! *warn-on-reflection* true)

(declare clj-kstream clj-ktable clj-kgroupedtable)

(def ^:private kstream-memo
  "Returns a kstream for the topic, creating a new one if needed."
  (memoize
   (fn [topology-builder
        {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
     (clj-kstream
      (.stream ^KStreamBuilder topology-builder
               key-serde
               value-serde
               (into-array String [name]))))))

(def ^:private ktable-memo
  "Returns a ktable for the topic, creating a new one if needed."
  (memoize
   (fn [topology-builder
        {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.table ^KStreamBuilder topology-builder key-serde value-serde name)))))

(deftype CljKStreamBuilder [^KStreamBuilder topology-builder]
  ITopologyBuilder
  (merge
    [_ kstreams]
    (clj-kstream
     (.merge topology-builder
             (into-array KStream (mapv kstream* kstreams)))))

  (new-name
    [_ prefix]
    (.newName topology-builder prefix))

  (kstream
    [_ topic-config]
    (kstream-memo topology-builder topic-config))

  (kstreams
    [_ topic-configs]
    (clj-kstream
     (let [topic-names (clojure.core/map :topic.metadata/name topic-configs)]
       (.stream topology-builder
                (into-array String topic-names)))))

  (ktable
    [_ topic-config]
    (ktable-memo topology-builder topic-config))

  (source-topics
    [_ application-id]
    (.sourceTopics topology-builder application-id))

  (topology-builder*
    [_]
    topology-builder))

(defn topology-builder
  "Makes a kstream builder."
  []
  (CljKStreamBuilder. (KStreamBuilder.)))

(deftype CljKStream [^KStream kstream]
  IKStreamBase
  (left-join
    [_ ktable value-joiner-fn]
    (clj-kstream
     (.leftJoin kstream
                (ktable* ktable)
                (value-joiner value-joiner-fn))))

  (for-each!
    [_ foreach-fn]
    (.foreach kstream (foreach-action foreach-fn))
    nil)

  (filter
    [_ predicate-fn]
    (clj-kstream
     (.filter kstream (predicate predicate-fn))))

  (filter-not
    [_ predicate-fn]
    (clj-kstream
     (.filterNot kstream (predicate predicate-fn))))

  (map-values
    [_ value-mapper-fn]
    (clj-kstream
     (.mapValues kstream (value-mapper value-mapper-fn))))

  (print!
    [_]
    (.print kstream)
    nil)

  (print!
    [_ {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.print kstream key-serde value-serde)
    nil)

  (through
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-kstream
     (.through kstream key-serde value-serde name)))

  (through
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-kstream
     (.through kstream key-serde value-serde (stream-partitioner partition-fn) name)))

  (to!
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.to kstream key-serde value-serde name)
    nil)

  (to!
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.to kstream key-serde value-serde (stream-partitioner partition-fn) name)
    nil)

  (write-as-text!
    [_ file-path]
    (.writeAsText kstream file-path))

  (write-as-text!
    [_ file-path {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.writeAsText kstream file-path key-serde value-serde))

  IKStream
  (aggregate-by-key
    [_ initializer-fn aggregator-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.aggregateByKey kstream
                      (initializer initializer-fn)
                      (aggregator aggregator-fn)
                      ^Serde key-serde
                      ^Serde value-serde
                      ^String name)))

  (aggregate-by-key-windowed
    [_ initializer-fn aggregator-fn windows]
    (clj-ktable
     (.aggregateByKey kstream
                      (initializer initializer-fn)
                      (aggregator aggregator-fn)
                      ^Windows windows)))

  (aggregate-by-key-windowed
    [_ initializer-fn aggregator-fn windows  {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.aggregateByKey kstream
                      (initializer initializer-fn)
                      (aggregator aggregator-fn)
                      ^Windows windows
                      ^Serde key-serde
                      ^Serde value-serde)))

  (branch
    [_ predicate-fns]
    (mapv clj-kstream
          (->> (into-array Predicate (mapv predicate predicate-fns))
               (.branch kstream))))

  (count-by-key
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde]}]
    (clj-ktable
     (.countByKey kstream ^Serde key-serde ^String name)))

  (count-by-key-windowed
    [_ windows]
    (clj-ktable
     (.countByKey kstream ^Windows windows)))

  (count-by-key-windowed
    [_ windows {:keys [kafka.serdes/key-serde]}]
    (clj-ktable
     (.countByKey kstream ^Windows windows ^Serde key-serde)))

  (flat-map
    [_ key-value-mapper-fn]
    (clj-kstream
     (.flatMap kstream (key-value-flatmapper key-value-mapper-fn))))

  (flat-map-values
    [_ value-mapper-fn]
    (clj-kstream
     (.flatMapValues kstream (value-mapper value-mapper-fn))))

  (join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.join kstream
            (kstream* other-kstream)
            (value-joiner value-joiner-fn)
            windows)))

  (join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :kafka.serdes/key-serde this-value-serde :kafka.serdes/value-serde}
     {other-value-serde :kafka.serdes/value-serde}]
    (clj-kstream
     (.join kstream
            (kstream* other-kstream)
            (value-joiner value-joiner-fn)
            windows
            key-serde
            this-value-serde
            other-value-serde)))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.leftJoin kstream
                (kstream* other-kstream)
                (value-joiner-fn)
                windows)))

  (left-join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :kafka.serdes/key-serde}
     {other-value-serde :kafka.serdes/value-serde}]
    (clj-kstream
     (.leftJoin kstream
                (kstream* other-kstream)
                (value-joiner value-joiner-fn)
                windows
                key-serde
                other-value-serde)))

  (map
    [_ key-value-mapper-fn]
    (clj-kstream
     (.map kstream (key-value-mapper key-value-mapper-fn))))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows]
    (clj-kstream
     (.outerJoin kstream
                 (kstream* other-kstream)
                 (value-joiner value-joiner-fn)
                 windows)))

  (outer-join-windowed
    [_ other-kstream value-joiner-fn windows
     {key-serde :kafka.serdes/key-serde value-serde :kafka.serdes/value-serde}
     {other-value-serde :kafka.serdes/value-serde}]
    (clj-kstream
     (.outerJoin kstream
                 (kstream* other-kstream)
                 (value-joiner value-joiner-fn)
                 windows
                 key-serde
                 value-serde
                 other-value-serde)))

  (process!
    [_ processor-supplier-fn state-store-names]
    (.process kstream
              (processor-supplier processor-supplier-fn)
              (into-array String state-store-names)))

  (reduce-by-key
   [_ reducer-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.reduceByKey kstream
                   (reducer reducer-fn)
                   ^Serde key-serde
                   ^Serde value-serde
                   ^String name)))

  (reduce-by-key-windowed
    [_ reducer-fn windows]
    (clj-ktable
     (.reduceByKey kstream (reducer reducer-fn) ^Windows windows)))

  (reduce-by-key-windowed
   [_ reducer-fn windows {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.reduceByKey kstream
                   (reducer reducer-fn)
                   ^Windows windows
                   ^Serde key-serde
                   ^Serde value-serde)))

  (select-key
    [_ select-key-value-mapper-fn]
    (clj-kstream
     (.selectKey kstream (select-key-value-mapper select-key-value-mapper-fn))))

  (transform
    [_ transformer-supplier-fn state-store-names]
    (clj-kstream
     (.transform kstream
                 (transformer-supplier transformer-supplier-fn)
                 (into-array String state-store-names))))

  (transform-values
    [_ transformer-supplier-fn state-store-names]
    (clj-kstream
     (.transformValues kstream
                       (transformer-supplier transformer-supplier-fn)
                       (into-array String state-store-names))))

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
     (.leftJoin ktable
                (ktable* other-ktable)
                (value-joiner value-joiner-fn))))

  (for-each!
    [_ foreach-fn]
    (.foreach ktable (foreach-action foreach-fn))
    nil)

  (filter
    [_ predicate-fn]
    (clj-ktable
     (.filter ktable (predicate predicate-fn))))

  (filter-not
    [_ predicate-fn]
    (clj-ktable
     (.filterNot ktable (predicate predicate-fn))))

  (map-values
    [_ value-mapper-fn]
    (clj-ktable
     (.mapValues ktable (value-mapper value-mapper-fn))))

  (print!
    [_]
    (.print ktable)
    nil)

  (print!
    [_ {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.print ktable key-serde value-serde)
    nil)

  (through
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.through ktable key-serde value-serde name)))

  (through
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.through ktable key-serde value-serde (stream-partitioner partition-fn) name)))

  (to!
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.to ktable key-serde value-serde name)
    nil)

  (to!
    [_ partition-fn {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.to ktable key-serde value-serde (stream-partitioner partition-fn) name)
    nil)

  (write-as-text!
    [_ file-path]
    (.writeAsText ktable file-path))

  (write-as-text!
    [_ file-path {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (.writeAsText ktable file-path key-serde value-serde))

  IKTable
  (group-by
    [_ key-value-mapper-fn]
    (clj-kgroupedtable
     (.groupBy ktable (key-value-mapper key-value-mapper-fn))))

  (group-by
    [_ key-value-mapper-fn {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-kgroupedtable
     (.groupBy ktable
               (key-value-mapper key-value-mapper-fn)
               key-serde
               value-serde)))

  (join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.join ktable
            (ktable* other-ktable)
            (value-joiner value-joiner-fn))))

  (outer-join
    [_ other-ktable value-joiner-fn]
    (clj-ktable
     (.outerJoin ktable
                 (ktable* other-ktable)
                 (value-joiner value-joiner-fn))))

  (to-kstream
    [_]
    (clj-kstream
     (.toStream ktable)))

  (to-kstream
    [_ key-value-mapper-fn]
    (clj-kstream
     (.toStream ktable (key-value-mapper key-value-mapper-fn))))

  (ktable* [_]
    ktable))

(defn clj-ktable
  "Makes a CljKTable object."
  [ktable]
  (CljKTable. ktable))

(deftype CljKGroupedTable [^KGroupedTable kgroupedtable]
  IKGroupedTable
  (aggregate
    [_ initializer-fn adder-fn subtractor-fn
     {:keys [topic.metadata/name kafka.serdes/value-serde]}]
    (clj-ktable
     (.aggregate kgroupedtable
                 (initializer initializer-fn)
                 (aggregator adder-fn)
                 (aggregator subtractor-fn)
                 value-serde
                 name)))

  (count
    [_ name]
    (clj-ktable
     (.count kgroupedtable name)))

  (reduce
    [_ adder-fn subtractor-fn {:keys [topic.metadata/name]}]
    (clj-ktable
     (.reduce kgroupedtable
              (reducer adder-fn)
              (reducer subtractor-fn)
              name)))

  (kgroupedtable*
    [_]
    kgroupedtable))

(defn clj-kgroupedtable
  "Makes a CljKGroupedTable object."
  [kgroupedtable]
  (CljKGroupedTable. kgroupedtable))
