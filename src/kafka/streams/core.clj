(ns kafka.streams.core
  "Clojure wrapper to kafka streams."
  (:refer-clojure :exclude [count map reduce group-by merge filter])
  (:require [kafka.streams.lambdas :refer :all])
  (:import org.apache.kafka.common.serialization.Serde
           org.apache.kafka.streams.KafkaStreams
           [org.apache.kafka.streams.kstream KGroupedTable KStream KStreamBuilder KTable Predicate Windows]
           org.apache.kafka.streams.processor.TopologyBuilder))

(set! *warn-on-reflection* true)

(defprotocol ITopologyBuilder
  "ITopologyBuilder provides the Kafka Streams DSL for users to specify
  computational logic and translates the given logic to a
  org.apache.kafka.streams.processor.internals.ProcessorTopology."
  (merge
    [topology-builder kstreams]
    "Create a new instance of KStream by merging the given streams.")

  (new-name
    [topology-builder prefix]
    "Create a unique processor name used for translation into the processor
    topology.")

  (kstream
    [topology-builder topic-config]
    "Create a KStream instance from the specified topic.")

  (kstreams
    [topology-builder topic-configs]
    "Create a KStream instance from the specified topics.")

  (ktable
    [topology-builder topic-config]
    "Create a KTable instance for the specified topic.")

  (topology-builder*
    [topology-builder]
    "Returns the underlying kstream builder."))

(defprotocol IKStreamBase
  "Shared methods."
  (left-join
    [kstream ktable value-joiner-fn]
    "Combine values of this stream with KTable's elements of the same key using Left Join.")

  (for-each!
    [kstream foreach-fn]
    "Perform an action on each element of KStream.")

  (filter
    [kstream predicate-fn]
    "Create a new instance of KStream that consists of all elements of this
    stream which satisfy a predicate.")

  (filter-not
    [kstream predicate-fn]
    "Create a new instance of KStream that consists all elements of this stream
    which do not satisfy a predicate.")

  (map-values
    [kstream value-mapper-fn]
    "Create a new instance of KStream by transforming the value of each element
    in this stream into a new value in the new stream.")

  (print!
    [kstream]
    [kstream topic-config]
    "Print the elements of this stream to *out*.")

  (through
    [kstream topic-config]
    [kstream partition-fn topic-config]
    "Materialize this stream to a topic, also creates a new instance of KStream
    from the topic.")

  (to!
    [kstream topic-config]
    [kstream partition-fn topic-config]
    "Materialize this stream to a topic.")

  (write-as-text!
    [kstream file-path]
    [kstream file-path topic-config]
    "Write the elements of this stream to a file at the given path."))

(defprotocol IKStream
  "KStream is an abstraction of a record stream of key-value pairs.

  A KStream is either defined from one or multiple Kafka topics that are
  consumed message by message or the result of a KStream transformation. A
  KTable can also be converted into a KStream.

  A KStream can be transformed record by record, joined with another KStream or
  KTable, or can be aggregated into a KTable."

  (aggregate-by-key
    [kstream initializer-fn aggregator-fn topic-config]
    "Aggregate values of this stream by key into a new instance of ever-updating KTable.")

  (aggregate-by-key-windowed
    [kstream initializer-fn aggregator-fn windows]
    [kstream initializer-fn aggregator-fn windows  topic-config]
    "Aggregate values of this stream by key on a window basis into a new
    instance of windowed KTable.")

  (branch
    [kstream predicate-fns]
    "Creates an array of KStream from this stream by branching the elements in
    the original stream based on the supplied predicates.")

  (count-by-key
    [kstream topic-config]
    "Count number of records of this stream by key into a new instance of
    ever-updating KTable.")

  (count-by-key-windowed
    [kstream windows]
    [kstream windows topic-config]
    "Count number of records of this stream by key into a new instance of
    ever-updating KTable.")

  (flat-map
    [kstream key-value-mapper-fn]
    "Create a new instance of KStream by transforming each element in this
    stream into zero or more elements in the new stream.")

  (flat-map-values
    [kstream value-mapper-fn]
    "Create a new instance of KStream by transforming the value of each element
    in this stream into zero or more values with the same key in the new
    stream.")

  (join-windowed
    [kstream other-kstream value-joiner-fn windows]
    [kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
    "Combine element values of this stream with another KStream's elements of
    the same key using windowed Inner Join." )

  (left-join-windowed
    [kstream other-ktable value-joiner-fn windows]
    [kstream other-ktable value-joiner-fn windows this-topic-config other-topic-config]
    "Combine values of this stream with KTable's elements of the same key using Left Join.")

  (map
    [kstream key-value-mapper-fn]
    "Create a new instance of KStream by transforming each element in this
    stream into a different element in the new stream.")

  (outer-join-windowed
    [kstream other-kstream value-joiner-fn windows]
    [kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
    "Combine values of this stream with another KStream's elements of the same
    key using windowed Outer Join." )

  (process!
    [kstream processor-supplier-fn state-store-names]
    "Process all elements in this stream, one element at a time, by applying a
    Processor.")

  (reduce-by-key
    [kstream reducer-fn topic-config]
    "Combine values of this stream by key into a new instance of ever-updating
    KTable.")

  (reduce-by-key-windowed
    [kstream reducer-fn windows]
    [kstream reducer-fn windows topic-config]
    "Combine values of this stream by key into a new instance of ever-updating
    KTable.")

  (select-key
    [kstream key-value-mapper-fn]
    "Create a new key from the current key and value.")

  (transform
    [kstream transformer-supplier-fn state-store-names]
    "Create a new KStream instance by applying a Transformer to all elements in
    this stream, one element at a time.")

  (transform-values
    [kstream transformer-supplier-fn state-store-names]
    "Create a new KStream instance by applying a ValueTransformer to all values
    in this stream, one element at a time.")

  (kstream*
    [kstream]
    "Return the underlying KStream object."))

(defprotocol IKTable
  "KTable is an abstraction of a changelog stream from a primary-keyed table.
  Each record in this stream is an update on the primary-keyed table with the
  record key as the primary key.

  A KTable is either defined from one or multiple Kafka topics that are consumed
  message by message or the result of a KTable transformation. An aggregation of
  a KStream also yields a KTable.

  A KTable can be transformed record by record, joined with another KTable or
  KStream, or can be re-partitioned and aggregated into a new KTable."
  (group-by
    [ktable key-value-mapper-fn]
    [ktable key-value-mapper-fn topic-config]
    "Group the records of this KTable using the provided KeyValueMapper.")

  (join
    [ktable other-ktable value-joiner-fn]
    "Combine values of this stream with another KTable stream's elements of the
    same key using Inner Join.")

  (outer-join
    [ktable other-ktable value-joiner-fn]
    "Combine values of this stream with another KStream's elements of the same
    key using Outer Join." )

  (to-kstream
    [ktable]
    [ktable key-value-mapper-fn]
    "Convert this stream to a new instance of KStream.")

  (ktable*
    [ktable]
    "Returns the underlying KTable object."))

(defprotocol IKGroupedTable
  "KGroupedTable is an abstraction of a grouped changelog stream from a
  primary-keyed table, usually on a different grouping key than the original
  primary key.

  It is an intermediate representation after a re-grouping of a KTable before an
  aggregation is applied to the new partitions resulting in a new KTable."
  (aggregate
    [kgroupedtable initializer-fn adder-fn subtractor-fn
     topic-config]
    "Aggregate updating values of this stream by the selected key into a new
    instance of KTable.")

  (count
    [kgroupedtable]
    "Count number of records of this stream by the selected key into a new
    instance of KTable.")

  (reduce
    [kgroupedtable adder-fn subtractor-fn topic-config]
    "Combine updating values of this stream by the selected key into a new
    instance of KTable.")

  (kgroupedtable*
    [kgroupedtable]
    "Returns the underlying KGroupedTable object."))

(declare clj-kstream clj-ktable clj-kgroupedtable)

(deftype CljKStreamBuilder [^KStreamBuilder topology-builder]
  ITopologyBuilder
  (merge
    [_ kstream]
    (into []
          #(clojure.core/map clj-kstream %)
          (.merge topology-builder
                  (into-array KStream [kstream]))))

  (new-name
    [_ prefix]
    (.newName topology-builder prefix))

  (kstream
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-kstream
     (.stream topology-builder
              key-serde
              value-serde
              (into-array String [name]))))

  (kstreams
    [_ topic-configs]
    (clj-kstream
     (let [topic-names (map :topic.metadata/name topic-configs)]
       (.stream topology-builder
                (into-array String topic-names)))))

  (ktable
    [_ {:keys [topic.metadata/name kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-ktable
     (.table topology-builder key-serde value-serde name)))

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
    (clj-kstream
     (.aggregateByKey kstream
                      (initializer initializer-fn)
                      (aggregator aggregator-fn)
                      ^Serde key-serde
                      ^Serde value-serde
                      ^String name)))

  (aggregate-by-key-windowed
    [_ initializer-fn aggregator-fn windows]
    (clj-kstream
     (.aggregateByKey kstream
                      (initializer initializer-fn)
                      (aggregator aggregator-fn)
                      ^Windows windows)))

  (aggregate-by-key-windowed
    [_ initializer-fn aggregator-fn windows  {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
    (clj-kstream
     (.aggregateByKey kstream
                      (initializer initializer-fn)
                      (aggregator aggregator-fn)
                      ^Windows windows
                      ^Serde key-serde
                      ^Serde value-serde)))

  (branch
    [_ predicate-fns]
    (->> predicate-fns
         (clojure.core/map predicate)
         (into-array Predicate)
         (.branch kstream)
         (clojure.core/map clj-kstream)
         (into [])))

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
    [_ windows key-serde]
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
    [_ key-value-mapper-fn]
    (clj-kstream
     (.selectKey kstream (key-value-mapper key-value-mapper-fn))))

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
    (.to ktable key-serde value-serde (stream-partitioner partition-fn name))
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
    [_]
    (clj-ktable
     (count kgroupedtable)))

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

(defn kafka-streams
  "Makes a Kafka Streams object."
  [builder opts]
  (KafkaStreams. ^TopologyBuilder (topology-builder* builder)
                 ^java.util.Properties opts))

(defn start!
  "Starts processing."
  [kafka-streams]
  (.start ^KafkaStreams kafka-streams))

(defn close!
  "Stops the kafka streams."
  [kafka-streams]
  (.close ^KafkaStreams kafka-streams))
