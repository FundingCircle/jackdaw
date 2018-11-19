(ns jackdaw.streams.protocols
  "Kafka streams protocols."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [count map merge reduce group-by filter peek])
  (:import org.apache.kafka.streams.KafkaStreams
           org.apache.kafka.streams.StreamsBuilder))

(defprotocol IStreamsBuilder
  (kstream
    [topology-builder topic-config]
    [topology-builder topic-config topic-pattern]
    "Create a KStream instance from the specified topic.")

  (kstreams
    [topology-builder topic-configs]
    "Create a KStream instance from the specified topics.")

  (ktable
    [topology-builder topic-config]
    [topology-builder topic-config store-name]
    "Create a KTable instance for the specified topic.")

  (global-ktable
    [topology-builder topic-config]
    "Creates a GlobalKTable that will consist of data from the specified
    topic.")

  (source-topics
    [topology-builder]
    "Get the names of topics that are to be consumed by the source nodes created
    by this builder.")

  (streams-builder*
    [streams-builder]
    "Returns the underlying KStreamBuilder."))

(defprotocol IKStreamBase
  "Shared methods."
  (left-join
    [kstream-or-ktable ktable value-joiner-fn]
    [kstream-or-ktable ktable value-joiner-fn topic-config]
    "Creates a KStream from the result of calling `value-joiner-fn` with
    each element in the KStream and the value in the KTable with the same
    key.")

  (filter
    [kstream-or-ktable predicate-fn]
    "Creates a KStream that consists of all elements that satisfy a
    predicate.")

  (filter-not
    [kstream-or-ktable predicate-fn]
    "Creates a KStream that consists of all elements that do not satisfy a
    predicate.")

  (group-by
    [ktable-or-ktable key-value-mapper-fn]
    [ktable-or-ktable key-value-mapper-fn topic-config]
    "Groups the records of this KStream/KTable using the key-value-mapper-fn.")

  (peek
    [kstream-or-ktable peek-fn]
    "Performs `peek-fn` on each element of the input stream.")

  (map-values
    [kstream-or-ktable value-mapper-fn]
    "Creates a KStream that is the result of calling `value-mapper-fn` on each
    element of the input stream.")

  (write-as-text!
    [kstream-or-ktable file-path]
    [kstream-or-ktable file-path topic-config]
    "Writes the elements of a stream to a file at the given path."))

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
    [kstream initializer-fn aggregator-fn windows topic-config]
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

  (for-each!
    [kstream foreach-fn]
    "Performs an action on each element of KStream.")

  (print!
    [kstream]
    "Prints the elements of the stream to *out*.")

  (through
    [kstream topic-config]
    "Materializes a stream to a topic, and returns a new KStream that will
    consume messages from the topic.")

  (to!
    [kstream topic-config]
    "Materializes a stream to a topic.")

  (group-by-key
    [kstream]
    [kstream topic-config]
    "Group the records with the same key into a KGroupedStream while preserving
    the original values.")

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

  (merge
    [kstream other]
    "Creates a KStream that has the records from both streams.")

  (outer-join-windowed
    [kstream other-kstream value-joiner-fn windows]
    [kstream other-kstream value-joiner-fn windows this-topic-config other-topic-config]
    "Combine values of this stream with another KStream's elements of the same
    key using windowed Outer Join." )

  (process!
    [kstream processor-fn state-store-names]
    "Process all elements in this stream, one element at a time, by applying a
    Processor.")

  (reduce-by-key
    [kstream reducer-fn topic-config]
    "Combine values of this stream by key into a new instance of ever-updating
    KTable.")

  (reduce-by-key-windowed
    [kstream reducer-fn windows topic-config]
    "Combine values of this stream by key into a new instance of ever-updating
    KTable.")

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
    "Create a new KStream instance by applying a Transformer to all elements in
    this stream, one element at a time.")

  (transform-values
    [kstream value-transformer-supplier-fn]
    [kstream value-transformer-supplier-fn state-store-names]
    "Create a new KStream instance by applying a ValueTransformer to all values
    in this stream, one element at a time.")

  (join-global
    [kstream global-ktable kv-mapper joiner])

  (left-join-global
    [kstream global-ktable kv-mapper joiner])

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

(defprotocol IKGroupedBase
  "Methods shared between `IKGroupedTable` and `IKGroupedStream`."
  (aggregate
    [kgrouped initializer-fn adder-fn subtractor-fn topic-config]
    [kgrouped initializer-fn aggregator-fn topic-config]
    "Aggregate updating values of this stream by the selected key into a new
    instance of KTable.")

  (count
    [kgrouped]
    [kgrouped topic-config]
    "Counts the number of records by key into a new KTable.")

  (reduce
    [kgrouped adder-fn subtractor-fn topic-config]
    [kgrouped reducer-fn topic-config]
    "Combine updating values of this stream by the selected key into a new
    instance of KTable."))

(defprotocol IKGroupedTable
  "KGroupedTable is an abstraction of a grouped changelog stream from a
  primary-keyed table, usually on a different grouping key than the original
  primary key.

  It is an intermediate representation after a re-grouping of a KTable before an
  aggregation is applied to the new partitions resulting in a new KTable."
  (kgroupedtable*
    [kgroupedtable]
    "Returns the underlying KGroupedTable object."))

(defprotocol IKGroupedStream
  "KGroupedStream is an abstraction of a grouped stream."
  (windowed-by-time [kgroupedstream window])

  (windowed-by-session [kgroupedstream window])

  (kgroupedstream*
    [kgroupedstream]
    "Returns the underlying KGroupedStream object."))

(defprotocol ITimeWindowedKStream
  "ITimeWindowedKStream is an abstraction of a time windowed stream."

  (time-windowed-kstream*
    [ktime-windowed-kstream]
    "Returns the underlying TimeWindowedKStream object."))

(defprotocol ISessionWindowedKStream
  "ISessionWindowedKStream is an abstraction of a session windowed stream."

  (session-windowed-kstream*
    [ksession-windowed-kstream]
    "Returns the underlying SessionWindowedKStream object."))

(defprotocol IGlobalKTable
  (global-ktable*
    [globalktable]
    "Returns the underlying GlobalKTable"))
