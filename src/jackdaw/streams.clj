(ns jackdaw.streams
  "Kafka streams protocols."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [count map merge reduce group-by filter peek])
  (:require [clojure.string :as str]
            [jackdaw.streams.interop :as interop]
            [jackdaw.streams.protocols :as p])
  (:import org.apache.kafka.streams.KafkaStreams
           org.apache.kafka.streams.StreamsBuilder
           org.apache.kafka.streams.KafkaStreams$State
           org.apache.kafka.streams.Topology))

(set! *warn-on-reflection* true)

;; StreamsBuilder

(defn kstream
  "Creates a KStream that will consume messages from the specified topic."
  ([streams-builder topic-config]
   {:pre [(map? topic-config)]}
   (p/kstream streams-builder topic-config))
  ([streams-builder topic-config topic-pattern]
   {:pre [(map? topic-config)]}
   (p/kstream streams-builder topic-config topic-pattern)))

(defn kstreams
  "Creates a KStream that will consume messages from the specified topics."
  [streams-builder topic-configs]
  (p/kstreams streams-builder topic-configs))

(defn ktable
  "Creates a KTable that will consist of data from the specified topic."
  ([streams-builder topic-config]
   (p/ktable streams-builder topic-config))
  ([streams-builder topic-config store-name]
   (p/ktable streams-builder topic-config store-name)))

(defn global-ktable
  "Creates a GlobalKTable that will consist of data from the specified
  topic."
  [streams-builder topic-config]
  (p/global-ktable streams-builder topic-config))

(defn source-topics
  "Gets the names of source topics for the topology."
  [streams-builder]
  (p/source-topics streams-builder))

(defn with-kv-state-store
  "Adds a persistent state store to the topology with the configured name
  and serdes"
  [streams-builder store-config]
  (p/with-kv-state-store streams-builder store-config))

(defn streams-builder*
  "Returns the underlying KStreamBuilder."
  [streams-builder]
  (p/streams-builder* streams-builder))

;; IKStreamBase

(defn join
  "Combines the values of the KStream-or-KTable and the KTable that
  share the same key using an inner join."
  [kstream-or-ktable ktable value-joiner-fn]
  (p/join kstream-or-ktable ktable value-joiner-fn))

(defn left-join
  "Creates a KStream from the result of calling `value-joiner-fn` with
  each element in the KStream and the value in the KTable with the same
  key."
  ([kstream ktable value-joiner-fn]
   (p/left-join kstream ktable value-joiner-fn))
  ([kstream ktable value-joiner-fn this-topic-config other-topic-config]
   (p/left-join kstream ktable value-joiner-fn this-topic-config other-topic-config)))

(defn filter
  [kstream predicate-fn]
  (p/filter kstream predicate-fn))

(defn filter-not
  "Creates a KStream that consists of all elements that do not satisfy a
  predicate."
  [kstream predicate-fn]
  (p/filter-not kstream predicate-fn))

(defn group-by
  "Groups the records of this KStream/KTable using the key-value-mapper-fn."
  ([ktable key-value-mapper-fn]
   (p/group-by ktable key-value-mapper-fn))
  ([ktable key-value-mapper-fn topic-config]
   (p/group-by ktable key-value-mapper-fn topic-config)))

(defn peek
  "Performs the action defined by `peek-fn` on each element of the input
  KStream, returning that stream untransformed."
  [kstream peek-fn]
  (p/peek kstream peek-fn))

(defn map-values
  "Creates a KStream that is the result of calling `value-mapper-fn` on each
  element of the input stream."
  [kstream value-mapper-fn]
  (p/map-values kstream value-mapper-fn))

(defn print!
  "Prints the elements of the stream to *out*."
  [kstream]
  (p/print! kstream))

(defn through
  "Materializes a stream to a topic, and returns a new KStream that will
  consume messages from the topic."
  [kstream topic-config]
  (p/through kstream topic-config))

(defn to
  "Materializes a stream to a topic."
  [kstream topic-config]
  (p/to! kstream topic-config))

;; IKStream

(defn branch
  "Returns a list of KStreams, one for each of the `predicate-fns`
  provided."
  [kstream predicate-fns]
  (p/branch kstream predicate-fns))

(defn flat-map
  "Creates a KStream that will consist of the concatenation of messages
  returned by calling `key-value-mapper-fn` on each key/value pair in the
  input stream."
  [kstream key-value-mapper-fn]
  (p/flat-map kstream key-value-mapper-fn))

(defn flat-map-values
  "Creates a KStream that will consist of the concatenation of the values
  returned by calling `value-mapper-fn` on each value in the input stream."
  [kstream value-mapper-fn]
  (p/flat-map-values kstream value-mapper-fn))

(defn for-each!
  "Performs an action on each element of KStream."
  [kstream foreach-fn]
  (p/for-each! kstream foreach-fn))

(defn group-by-key
  "Groups records with the same key into a KGroupedStream."
  ([kstream]
   (p/group-by-key kstream))
  ([kstream topic-config]
   (p/group-by-key kstream topic-config)))

(defn join-windowed
  "Combines the values of two streams that share the same key using a
  windowed inner join."
  ([kstream other-kstream value-joiner-fn windows]
   (p/join-windowed kstream other-kstream value-joiner-fn windows))
  ([kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
   (p/join-windowed kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config)))

(defn left-join-windowed
  "Combines the values of two streams that share the same key using a
  windowed left join."
  ([kstream other-kstream value-joiner-fn windows]
   (p/left-join-windowed kstream other-kstream value-joiner-fn windows))
  ([kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
   (p/left-join-windowed kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config)))

(defn map
  "Creates a KStream that consists of the result of applying
  `key-value-mapper-fn` to each key/value pair in the input stream."
  [kstream key-value-mapper-fn]
  (p/map kstream key-value-mapper-fn))

(defn outer-join-windowed
  "Combines the values of two streams that share the same key using a
  windowed outer join."
  ([kstream other-kstream value-joiner-fn windows]
   (p/outer-join-windowed kstream other-kstream value-joiner-fn windows))
  ([kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
   (p/outer-join-windowed kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config)))

(defn process!
  "Applies `processor-fn` to each item in the input stream."
  [kstream processor-fn state-store-names]
  (p/process! kstream processor-fn state-store-names))

(defn select-key
  "Create a new key from the current key and value.

   `select-key-value-mapper-fn` should be a function that takes a key-value
   pair, and returns the value of the new key. Here is example multiplies each
   key by 10:

   ```(fn [[k v]] (* 10 k))```"
  [kstream select-key-value-mapper-fn]
  (p/select-key kstream select-key-value-mapper-fn))

(defn transform
  "Creates a KStream that consists of the results of applying the transformer
  to each key/value in the input stream."
  ([kstream transformer-supplier-fn]
   (p/transform kstream transformer-supplier-fn))
  ([kstream transformer-supplier-fn state-store-names]
   (p/transform kstream transformer-supplier-fn state-store-names)))

(defn flat-transform
  "Creates a KStream that consists of the results of applying the transformer
  to each value in the input stream. Result of the transform should be iterable,
  and the resulting stream is as per flatMap"
  ([kstream transformer-supplier-fn]
   (p/flat-transform kstream transformer-supplier-fn))
  ([kstream transformer-supplier-fn state-store-names]
   (p/flat-transform kstream transformer-supplier-fn state-store-names)))

(defn transform-values
  "Creates a KStream that consists of the results of applying the transformer
  to each value in the input stream."
  ([kstream value-transformer-supplier-fn]
   (p/transform-values kstream value-transformer-supplier-fn))
  ([kstream value-transformer-supplier-fn state-store-names]
   (p/transform-values kstream value-transformer-supplier-fn state-store-names)))

(defn flat-transform-values
  "Creates a KStream that consists of the results of applying the transformer
  to each value in the input stream. Result of the transform should be iterable,
  and the resulting stream is as per flatMap"
  ([kstream value-transformer-supplier-fn]
   (p/flat-transform-values kstream value-transformer-supplier-fn))
  ([kstream value-transformer-supplier-fn state-store-names]
   (p/flat-transform-values kstream value-transformer-supplier-fn state-store-names)))

(defn join-global
  [kstream global-ktable kv-mapper joiner]
  (p/join-global kstream global-ktable kv-mapper joiner))

(defn left-join-global
  [kstream global-ktable kv-mapper joiner]
  (p/left-join-global kstream global-ktable kv-mapper joiner))

(defn merge
  [kstream other]
  (p/merge kstream other))

(defn kstream*
  "Returns the underlying KStream object."
  [kstream]
  (p/kstream* kstream))

;; IKTable

(defn outer-join
  "Combines the values of two KTables that share the same key using an outer
  join."
  [ktable other-ktable value-joiner-fn]
  (p/outer-join ktable other-ktable value-joiner-fn))

(defn to-kstream
  "Converts a KTable to a KStream."
  ([ktable]
   (p/to-kstream ktable))
  ([ktable key-value-mapper-fn]
   (p/to-kstream ktable key-value-mapper-fn)))

(defn suppress
  "Suppress some updates from this changelog stream"
  [ktable suppressed]
  (p/suppress ktable suppressed))

(defn ktable*
  "Returns the underlying KTable object."
  [ktable]
  (p/ktable* ktable))

;; IKGroupedBase

(defn aggregate
  "Aggregates values by key into a new KTable."
  ([kgrouped initializer-fn adder-fn]
   (p/aggregate kgrouped initializer-fn adder-fn))
  ([kgrouped initializer-fn aggregator-fn subtractor-fn-or-topic-config]
   (p/aggregate kgrouped initializer-fn aggregator-fn subtractor-fn-or-topic-config))
  ([kgrouped initializer-fn adder-fn subtractor-or-merger-fn topic-config]
   (p/aggregate kgrouped initializer-fn adder-fn subtractor-or-merger-fn topic-config)))

(defn count
  "Counts the number of records by key into a new KTable."
  ([kgrouped]
   (p/count kgrouped))
  ([kgrouped name]
   (p/count kgrouped name)))

(defn reduce
  "Combines values of a stream by key into a new KTable."
  ([kgrouped adder-fn subtractor-fn topic-config]
   (p/reduce kgrouped adder-fn subtractor-fn topic-config))
  ([kgrouped reducer-fn subtractor-fn-or-topic-config]
   (p/reduce kgrouped reducer-fn subtractor-fn-or-topic-config))
  ([kgrouped reducer-fn]
   (p/reduce kgrouped reducer-fn)))

;; IKGroupedTable

(defn kgroupedtable*
  "Returns the underlying KGroupedTable object."
  [kgroupedtable]
  (p/kgroupedtable* kgroupedtable))

;; IKGroupedStream

(defn window-by-time
  "Windows the KStream"
  ([kgroupedstream window]
   (p/windowed-by-time kgroupedstream window)))

(defn window-by-session
  "Windows the KStream"
  ([kgroupedstream window]
   (p/windowed-by-session kgroupedstream window)))

(defn kgroupedstream*
  "Returns the underlying KGroupedStream object."
  ([kgroupedstream]
   (p/kgroupedstream* kgroupedstream)))

;; IGlobalKTable

(defn global-ktable*
  "Returns the underlying GlobalKTable"
  [globalktable]
  (p/global-ktable* globalktable))

(defn streams-builder
  []
  (interop/streams-builder))

(defn kafka-streams
  "Makes a Kafka Streams object."
  ([builder opts]
   (let [props (java.util.Properties.)]
     (.putAll props opts)
     (KafkaStreams. ^Topology (.build ^StreamsBuilder (streams-builder* builder))
                    ^java.util.Properties props))))

(defn start
  "Starts processing."
  [kafka-streams]
  (.start ^KafkaStreams kafka-streams))

(defn close
  "Stops the kafka streams."
  [kafka-streams]
  (.close ^KafkaStreams kafka-streams))

(defn state->keyword [^KafkaStreams$State state]
  (-> state .name str/lower-case (str/replace #"_" "-") keyword))

(defn state [^KafkaStreams k-streams]
  (-> k-streams .state state->keyword))
