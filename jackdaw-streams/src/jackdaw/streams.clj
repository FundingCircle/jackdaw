(ns jackdaw.streams
  "Kafka streams protocols."
  (:refer-clojure :exclude [count map reduce group-by merge filter])
  (:import org.apache.kafka.streams.KafkaStreams
           org.apache.kafka.streams.processor.TopologyBuilder))

(defprotocol ITopologyBuilder
  "ITopologyBuilder provides the entry points for the Kafka Streams DSL."
  (merge
    [topology-builder kstreams]
    "Merges another KStream with this one.")

  (new-name
    [topology-builder prefix]
    "Returns a unique processor name with the given prefix.")

  (kstream
    [topology-builder topic-config]
    [topology-builder topic-config topic-pattern]
    "Creates a KStream that will consume messages from the specified topic.")

  (kstreams
    [topology-builder topic-configs]
    "Creates KStreams that will consume messages from the specified topics.")

  (ktable
    [topology-builder topic-config]
    [topology-builder topic-config store-name]
    "Creates a KTable that will consist of data from the specified topic.")

  (global-ktable
    [topology-builder topic-config]
    [topology-builder topic-config store-name]
    "Creates a GlobalKTable that will consist of data from the specified
    topic.")

  (source-topics
    [topology-builder]
    "Gets the names of source topics for the topology.")

  (topology-builder*
    [topology-builder]
    "Returns the underlying KStreamBuilder."))

(defprotocol IKStreamBase
  "Methods common to KStream & KTable."
  (left-join
    [kstream ktable value-joiner-fn]
    [kstream ktable value-joiner-fn topic-config]
    "Creates a KStream from the result of calling `value-joiner-fn` with
    each element in the KStream and the value in the KTable with the same
    key.")

  (for-each!
    [kstream foreach-fn]
    "Performs an action on each element of KStream.")

  (filter
    [kstream predicate-fn]
    "Creates a KStream that consists of all elements that satisfy a
    predicate.")

  (filter-not
    [kstream predicate-fn]
    "Creates a KStream that consists of all elements that do not satisfy a
    predicate.")

  (group-by
    [ktable key-value-mapper-fn]
    [ktable key-value-mapper-fn topic-config]
    "Groups the records of this KStream/KTable using the key-value-mapper-fn.")

  (map-values
    [kstream value-mapper-fn]
    "Creates a KStream that is the result of calling `value-mapper-fn` on each
    element of the input stream.")

  (print!
    [kstream]
    [kstream topic-config]
    "Prints the elements of the stream to *out*.")

  (through
    [kstream topic-config]
    [kstream partition-fn topic-config]
    "Materializes a stream to a topic, and returns a new KStream that will
    consume messages from the topic.")

  (to!
    [kstream topic-config]
    [kstream partition-fn topic-config]
    "Materializes a stream to a topic.")

  (write-as-text!
    [kstream file-path]
    [kstream file-path topic-config]
    "Writes the elements of a stream to a file at the given path."))

(defprotocol IKStream
  "A KStream is an abstraction of a stream of key-value pairs."
  (branch
    [kstream predicate-fns]
    "Returns a list of KStreams, one for each of the `predicate-fns`
    provided.")

  (flat-map
    [kstream key-value-mapper-fn]
    "Creates a KStream that will consist of the concatentation of messages
    returned by calling `key-value-mapper-fn` on each key/value pair in the
    input stream.")

  (flat-map-values
    [kstream value-mapper-fn]
    "Creates a KStream that will consist of the concatentation of the values
    returned by calling `value-mapper-fn` on each value in the input stream.")

  (group-by-key
    [kstream]
    [kstream topic-config]
    "Groups records with the same key into a KGroupedStream.")

  (join-windowed
    [kstream other-kstream value-joiner-fn windows]
    [kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
    "Combines the values of two streams that share the same key using a
    windowed inner join.")

  (left-join-windowed
    [kstream other-kstream value-joiner-fn windows]
    [kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
    "Combines the values of two streams that share the same key using a
    windowed left join.")

  (map
    [kstream key-value-mapper-fn]
    "Creates a KStream that consists of the result of applying
    `key-value-mapper-fn` to each key/value pair in the input stream.")

  (outer-join-windowed
    [kstream other-kstream value-joiner-fn windows]
    [kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
    "Combines the values of two streams that share the same key using a
    windowed outer join.")

  (process!
    [kstream processor-fn state-store-names]
    "Applies `processor-fn` to each item in the input stream.")

  (select-key
    [kstream select-key-value-mapper-fn]
    "Create a new key from the current key and value.

    `select-key-value-mapper-fn` should be a function that takes a key-value
    pair, and returns the value of the new key. Here is example multiplies each
    key by 10:

    ```(fn [[k v]] (* 10 k))```")

  (transform
    [kstream transformer-supplier-fn]
    [kstream transformer-supplier-fn state-store-names]
    "Creates a KStream that consists of the results of applying the transformer
    to each key/value in the input stream.")

  (transform-values
    [kstream value-transformer-supplier-fn]
    [kstream value-transformer-supplier-fn state-store-names]
    "Creates a KStream that consists of the results of applying the transformer
    to each value in the input stream.")

  (join-global
    [kstream global-kstream kv-mapper joiner])

  (left-join-global
    [kstream global-kstream kv-mapper joiner])

  (kstream*
    [kstream]
    "Returns the underlying KStream object."))

(defprotocol IKTable
  "A Ktable is an abstraction of a changlog stream."
  (join
    [ktable other-ktable value-joiner-fn]
    "Combines the values of the two KTables that share the same key using an
    inner join.") 

  (outer-join
    [ktable other-ktable value-joiner-fn]
    "Combines the values of two KTables that share the same key using an outer
    join." )

  (to-kstream
    [ktable]
    [ktable key-value-mapper-fn]
    "Converts a KTable to a KStream.")

  (ktable*
    [ktable]
    "Returns the underlying KTable object."))

(defprotocol IKGroupedBase
  "Methods shared between `IKGroupedTable` and `IKGroupedStream`."
  (aggregate
    [kgrouped initializer-fn adder-fn subtractor-fn topic-config]
    [kgrouped initializer-fn aggregator-fn topic-config]
    "Aggregates values by key into a new KTable.")

  (count
    [kgrouped name]
    "Counts the number of records by key into a new KTable.")

  (reduce
    [kgrouped adder-fn subtractor-fn topic-config]
    [kgrouped reducer-fn topic-config]
    "Combines values of a stream by key into a new KTable."))

(defprotocol IKGroupedTable
  "KGroupedTable is an abstraction of a grouped changelog stream."
  (kgroupedtable*
    [kgroupedtable]
    "Returns the underlying KGroupedTable object."))

(defprotocol IKGroupedStream
  "KGroupedStream is an abstraction of a grouped stream."
  (aggregate-windowed
    [kgroupedstream initializer-fn aggregator-fn windows topic-config])

  (count-windowed
    [kgroupedstream windows topic-config]
    "Counts the number of records by key into a new KTable.")

  (reduce-windowed
    [kgroupedstream reducer-fn windows topic-config]
    "Combines values of the stream by key into a new KTable.")

  (kgroupedstream*
    [kgroupedstream]
    "Returns the underlying KGroupedStream object."))

(defprotocol IGlobalKTable
  (global-ktable*
    [globalktable]
    "Returns the underlying GlobalKTable"))

(defn kafka-streams
  "Makes a Kafka Streams object."
  ([builder opts]
   (let [props (java.util.Properties.)]
     (.putAll props opts)
     (KafkaStreams. ^TopologyBuilder (topology-builder* builder)
                    ^java.util.Properties props))))

(defn start!
  "Starts processing."
  [kafka-streams]
  (.start ^KafkaStreams kafka-streams))

(defn close!
  "Stops the kafka streams."
  [kafka-streams]
  (.close ^KafkaStreams kafka-streams))
