(ns jackdaw.client
  "Clojure wrapper to Kafka's consumer and producer APIs.

  The consumers and producers are the basis for streams, and many
  other use cases. They can be used to send messages directly to, or
  read messages from topics. There are also some facilities for
  polling, and transactions.

  See `jackdaw.client.*` for some add-ons atop this API.
  "
  {:license "3-clause BSD <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.tools.logging :as log]
            [jackdaw.data :as jd])
  (:import java.time.Duration
           java.util.Collection
           [org.apache.kafka.clients.consumer
            Consumer KafkaConsumer OffsetAndTimestamp]
           [org.apache.kafka.clients.producer
            Callback KafkaProducer Producer ProducerRecord]
           [org.apache.kafka.common
            PartitionInfo TopicPartition]
           org.apache.kafka.common.serialization.Serde))

(set! *warn-on-reflection* true)
(declare assignment)

;;;; Producer

(defn producer
  "Return a producer with the supplied properties and optional Serdes."
  (^KafkaProducer [config]
   (KafkaProducer. ^java.util.Properties (jd/map->Properties config)))
  (^KafkaProducer [config {:keys [^Serde key-serde ^Serde value-serde]}]
   (KafkaProducer. ^java.util.Properties (jd/map->Properties config)
                   (.serializer key-serde)
                   (.serializer value-serde))))

(defn callback
  "Return a kafka `Callback` function out of a clojure `fn`.

  The fn must be of 2-arity, being `[record-metadata?, ex?]` where the
  record-metadata may be the datafied metadata for the produced
  record, and the ex may be an exception encountered while producing
  the record.

  Callbacks are `void`, so the return value is ignored."
  ^Callback [on-completion]
  (reify Callback
    (onCompletion [this record-meta exception]
      (on-completion record-meta exception))))

(defn send!
  "Asynchronously sends a record to a topic, returning a `Future`
  which will produce a data structure describing the metadata of the
  produced record when forced.

  A 2-arity callback function may be provided. It will be invoked with
  either [RecordMetdata, nil] or [nil, Exception] respectively if the
  record was sent or if an exception was encountered."
  ([producer record]
   (let [send-future (.send ^Producer producer ^ProducerRecord record)]
     (delay (jd/datafy @send-future))))
  ([producer record callback-fn]
   (let [send-future (.send ^Producer producer
                            ^ProducerRecord record
                            ^Callback (callback callback-fn))]
     (delay (jd/datafy @send-future)))))

(defn produce!
  "Helper wrapping `#'send!`.

  Builds and sends a `ProducerRecord` so you don't have to. Returns
  a future which will produce datafied record metadata when forced."
  ([producer topic value]
   (send! producer
          (jd/->ProducerRecord topic value)))
  ([producer topic key value]
   (send! producer
          (jd/->ProducerRecord topic key value)))
  ([producer topic partition key value]
   (send! producer
          (jd/->ProducerRecord topic partition key value)))
  ([producer topic partition timestamp key value]
   (send! producer
          (jd/->ProducerRecord topic partition timestamp key value)))
  ([producer topic partition timestamp key value headers]
   (send! producer
          (jd/->ProducerRecord topic partition timestamp key value headers))))

;;;; Consumer

(defn consumer
  "Return a consumer with the supplied properties and optional Serdes."
  (^KafkaConsumer [config]
   (KafkaConsumer. ^java.util.Properties (jd/map->Properties config)))
  (^KafkaConsumer [config {:keys [^Serde key-serde ^Serde value-serde] :as t}]

   (when-not (or key-serde
                 (get config "key.deserializer"))
     (throw (ex-info "No key serde specified"
                     {:topic t, :config config})))

   (when-not (or value-serde
                 (get config "value.deserializer"))
     (throw (ex-info "No value serde specified"
                     {:topic t, :config config})))

   (KafkaConsumer.
    ^java.util.Properties (jd/map->Properties config)
    (when key-serde
      (.deserializer key-serde))
    (when value-serde
      (.deserializer value-serde)))))

(defn subscription
  "Return the subscription(s) of a consumer as a collection of topics.

  Subscriptions are a set of strings, being the names of topics which
  are subscribed to."
  [^KafkaConsumer consumer]
  (.subscription consumer))

(defn subscribe
  "Subscribe a consumer to the specified topics.

  Returns the consumer."
  ^KafkaConsumer [^KafkaConsumer consumer topic-configs]
  (.subscribe consumer
              ^Collection (mapv (fn [{:keys [topic-name] :as t}]
                                  (when-not (string? topic-name)
                                    (throw (ex-info "No name for topic!"
                                                    {:topic t})))
                                  topic-name)
                                topic-configs))
  consumer)

(defn subscribed-consumer
  "Given a broker configuration and topics, returns a consumer that is
  subscribed to all of the given topic descriptors.

  WARNING: All topics subscribed to by a single consumer must share a
  single pair of key and value serde instances. The serdes of the
  first requested topic are used, and all other topics are expected to
  be able to use same serdes."
  ^KafkaConsumer [config topic-configs]
  (when-not (sequential? topic-configs)
    (throw (ex-info "subscribed-consumer takes a seq of topics!"
                    {:topic-configs topic-configs})))

  (-> (consumer config (first topic-configs))
      (subscribe topic-configs)))

(defn partitions-for
  "Given a producer or consumer and a Jackdaw topic descriptor, return
  metadata about the partitions assigned to the given consumer or
  producer."
  [producer-or-consumer {:keys [^String topic-name]}]
  (->> (cond (instance? KafkaConsumer producer-or-consumer)
             (.partitionsFor ^KafkaConsumer producer-or-consumer topic-name)

             (instance? KafkaProducer producer-or-consumer)
             (.partitionsFor ^KafkaProducer producer-or-consumer topic-name)

             :else (throw (ex-info "Got non producer/consumer!"
                                   {:inst producer-or-consumer
                                    :class (class producer-or-consumer)})))
       (map jd/datafy)))

(defn num-partitions
  "Given a producer or consumer and a topic, return the number of
  partitions for that topic.

  Note that partitions are 0-indexed, so a number of partitions 1
  means that only partition 0 exists."
  [producer-or-consumer topic]
  (count (partitions-for producer-or-consumer topic)))

(defn poll
  "Polls kafka for new messages, returning a potentially empty sequence
  of datafied messages."
  [^Consumer consumer timeout]
  (some->> (if (int? timeout)
             (.poll consumer ^long timeout)
             (.poll consumer ^Duration timeout))
           (map jd/datafy)))

(defn position
  "Get the offset of the next record that will be fetched.

  Accepts either a `TopicPartition` record, or a datafied
  `TopicPartition` as generated by the rest of the Jackdaw API."
  ^long [^Consumer consumer topic-partition]
  (.position consumer (jd/as-TopicPartition topic-partition)))

(defn position-all
  "Call position on every assigned partition, producing a map from
  partition information to the consumer's offset into that partition."
  [consumer]
  (->> (assignment consumer)
       (map (juxt jd/datafy (partial position consumer)))
       (into {})))

(defn seek
  "Seek the consumer to the specified offset on the specified partition.

  Accepts either a `TopicPartition` instance or a datafied
  `TopicPartition` as produced by the rest of the Jackdaw API.

  Returns the consumer for convenience with `->`, `doto` etc."
  [^Consumer consumer topic-partition ^long offset]
  (doto ^Consumer consumer
    (.seek ^TopicPartition (jd/as-TopicPartition topic-partition) offset)))

(defn seek-to-end-eager
  "Seek to the last offset for all assigned partitions, and force positioning.

  When no partitions are passed, seek on all assigned partitions.

  Returns the consumer."
  ([^Consumer consumer]
   (seek-to-end-eager consumer []))
  ([^Consumer consumer topic-partitions]
   (poll consumer 0) ;; load assignments
   (.seekToEnd consumer topic-partitions)
   (position-all consumer)
   consumer))

(defn seek-to-beginning-eager
  "Seek to the first offset for the given topic/partitions and force positioning.

  When no partitions are passed, seek on all assigned
  topic-partitions."
  ([^Consumer consumer]
   (seek-to-beginning-eager consumer [])
   consumer)
  ([^Consumer consumer topic-partitions]
   (poll consumer 0)
   (.seekToBeginning consumer (map jd/as-TopicPartition topic-partitions))
   (position-all consumer)
   consumer))

(defn offsets-for-times
  "Given a subscribed consumer and a mapping of topic-partition or
  `TopicPartition` records to timestamps, return a mapping from
  topic-partition descriptors to the offset into each partition of the
  FIRST record whose timestamp is equal to or greater than the given
  timestamp.

  Timestamps are longs to MS precision in UTC."
  [^Consumer consumer partition-timestamps]
  (->> partition-timestamps
       (map (fn [[topic-partition ts]]
              [(jd/as-TopicPartition topic-partition) (long ts)]))
       (into {})
       (.offsetsForTimes consumer)
       (map (fn [[k v]] [k v]))
       (into {})))

(defn end-offsets
  [^Consumer consumer partitions]
  (->> partitions
       (map (fn [topic-partition]
              (jd/as-TopicPartition topic-partition)))
       (.endOffsets consumer)
       (map (fn [[k v]] [k v]))
       (into {})))

(defn seek-to-timestamp
  "Given an timestamp in epoch MS, a subscribed consumer and a seq of
  Jackdaw topics, seek all partitions of the selected topics to the
  offsets reported by Kafka to correlate with the given timestamp.

  After seeking, the first message read from each partition will be
  the EARLIEST message whose timestamp is greater than or equal to the
  timestamp sought.

  If no such message exists, the first message read from each partition
  will be the next new message written to that partition.

  Returns the consumer for convenience with `->`, `doto` etc."
  [^Consumer consumer timestamp topics]
  (let [topic-partitions (->> (mapcat #(partitions-for consumer %) topics)
                              (map #(select-keys % [:topic-name :partition])))
        ts-offsets       (offsets-for-times consumer
                                         (zipmap topic-partitions
                                                 (repeat (count topic-partitions) timestamp)))
        end-offsets      (end-offsets consumer topic-partitions)
        offsets          (reduce-kv (fn [m k v]
                                      (assoc m k {:ts-offset v
                                                  :end-offset (get end-offsets k)}))
                                    {} ts-offsets)]
    (doseq [[^TopicPartition topic-partition
             {:keys [^OffsetAndTimestamp ts-offset end-offset]}] offsets]
      (let [offset (or (when ts-offset
                         (.offset ts-offset))
                       (inc end-offset))]
        (log/infof "Setting starting offset (topic=%s, partition=%s): %s"
                   (.topic topic-partition)
                   (.partition topic-partition)
                   offset)
        (seek consumer topic-partition offset)))

    consumer))

(defn assign
  "Assign a consumer to specific partitions for specific topics. Returns the consumer."
  [^Consumer consumer & topic-partitions]
  (.assign consumer topic-partitions)
  consumer)

(defn assign-all
  "Assigns all of the partitions for all of the given topics to the consumer"
  [^Consumer consumer topics]
  (let [partitions (->> topics
                        (mapcat #(.partitionsFor consumer ^String %))
                        (map (fn [^PartitionInfo x]
                               (TopicPartition. (.topic x) (.partition x)))))]
    (apply assign consumer partitions)))

(defn assignment
  "Return the assigned topics and partitions of a consumer."
  [^KafkaConsumer consumer]
  (map jd/datafy (.assignment consumer)))
