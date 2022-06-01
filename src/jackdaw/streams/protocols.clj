(ns jackdaw.streams.protocols
  "Kafka streams protocols."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:refer-clojure :exclude [count map merge reduce group-by filter peek]))

(set! *warn-on-reflection* true)

(defprotocol IStreamsBuilder
  (kstream
    [topology-builder topic-config]
    [topology-builder topic-config topic-pattern]
    "Creates a KStream that will consume messages from the specified topic.")

  (kstreams
    [topology-builder topic-configs]
    "Creates a KStream that will consume messages from the specified topics.")

  (ktable
    [topology-builder topic-config]
    [topology-builder topic-config store-name]
    "Creates a KTable that will consist of data from the specified topic.")

  (global-ktable
    [topology-builder topic-config]
    "Creates a GlobalKTable that will consist of data from the specified
    topic.")

  (source-topics
    [topology-builder]
    "Gets the names of source topics for the topology.")

  (with-kv-state-store
    [topology-builder store-config]
    "Adds a persistent state store to the topology with the configured name
    and serdes.")
    
  (streams-builder*
    [streams-builder]
    "Returns the underlying KStreamBuilder."))

(defprotocol IKStreamBase
  "Methods common to KStream and KTable."
  (join
    [kstream-or-ktable ktable value-joiner-fn]
    "Combines the values of the KStream-or-KTable with the values of the
    KTable that share the same key using an inner join.")

  (left-join
    [kstream-or-ktable ktable value-joiner-fn]
    [kstream-or-ktable ktable value-joiner-fn this-topic-config other-topic-config]
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
  "A KStream is an abstraction of a stream of key-value pairs."
  (branch
    [kstream predicate-fns]
    "Returns a list of KStreams, one for each of the `predicate-fns`
    provided.")

  (flat-map
    [kstream key-value-mapper-fn]
    "Creates a KStream that will consist of the concatenation of messages
    returned by calling `key-value-mapper-fn` on each key/value pair in the
    input stream.")

  (flat-map-values
    [kstream value-mapper-fn]
    "Creates a KStream that will consist of the concatenation of the values
    returned by calling `value-mapper-fn` on each value in the input stream.")

  (for-each!
    [kstream foreach-fn]
    "Performs an action on each element of KStream.")

  (print!
    [kstream]
    "Prints the elements of the stream to *out*.")

  (through
    [kstream topic-config]
    "Materializes a stream to a topic, and returns a new KStream that will
    consume messages from the topic. Messages in the new topic will be partitioned
    based on the output of the optional partition function that represents StreamPartitioner class")

  (to!
    [kstream topic-config]
    "Materializes a stream to a topic.")

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

  (merge
    [kstream other]
    "Creates a KStream that has the records from both streams.")

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

  (flat-transform
    [kstream transformer-supplier-fn]
    [kstream transformer-supplier-fn state-store-names]
    "Creates a KStream that consists of the results of applying the transformer
    to each key/value in the input stream via flatTransform.")

  (transform-values
    [kstream value-transformer-supplier-fn]
    [kstream value-transformer-supplier-fn state-store-names]
    "Creates a KStream that consists of the results of applying the transformer
    to each value in the input stream.")

  (flat-transform-values
    [kstream value-transformer-supplier-fn]
    [kstream value-transformer-supplier-fn state-store-names]
    "Creates a KStream that consists of the results of applying the transformer
    to each key/value in the input stream via flatTransformValues.")

  (join-global
    [kstream global-ktable kv-mapper joiner])

  (left-join-global
    [kstream global-ktable kv-mapper joiner])

  (kstream*
    [kstream]
    "Returns the underlying KStream object."))

(defprotocol IKTable
  "A Ktable is an abstraction of a changlog stream."

  (outer-join
    [ktable other-ktable value-joiner-fn]
    "Combines the values of two KTables that share the same key using an outer
    join.")

  (to-kstream
    [ktable]
    [ktable key-value-mapper-fn]
    "Converts a KTable to a KStream.")

  (suppress
    [ktable {:keys [max-records max-bytes until-time-limit-ms]}]
    "Suppress some updates from this changelog stream.
    You can either specify `max-records` or `max-bytes`. If an empty map is
    passed, the suppress will be unbounded. If `until-time-limit-ms` is set,
    this will override the `TimeWindow` interval. Note that when relying on the
    configured `TimeWindow` the default `grace` period is `24h - window-size`.")

  (ktable*
    [ktable]
    "Returns the underlying KTable object."))

(defprotocol IKGroupedBase
  "Methods shared between `IKGroupedTable` and `IKGroupedStream`."
  (aggregate
    [kgrouped initializer-fn adder-fn subtractor-or-merger-fn topic-config]
    [kgrouped initializer-fn aggregator-fn subtractor-fn-or-topic-config]
    [kgrouped initializer-fn aggregator-fn]
    "Aggregates values by key into a new KTable.")

  (count
    [kgrouped]
    [kgrouped topic-config]
    "Counts the number of records by key into a new KTable.")

  (reduce
    [kgrouped adder-fn subtractor-fn topic-config]
    [kgrouped reducer-fn subtractor-fn-or-topic-config]
    [kgrouped reducer-fn]
    "Combines values of a stream by key into a new KTable."))

(defprotocol IKGroupedTable
  "KGroupedTable is an abstraction of a grouped changelog stream."
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
