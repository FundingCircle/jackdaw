(ns jackdaw.client.extras
  (:require [clojure.tools.logging :as log]
            [jackdaw.client :as jc]
            [jackdaw.serdes :as serdes])
  (:import (org.apache.kafka.clients.consumer Consumer
                                              ConsumerRecord
                                              KafkaConsumer
                                              OffsetAndTimestamp)
           (org.apache.kafka.clients.producer KafkaProducer)
           (org.apache.kafka.common TopicPartition
                                    PartitionInfo)
           org.apache.kafka.common.serialization.Serde
           org.apache.kafka.common.utils.Utils
           org.apache.kafka.streams.KafkaStreams))

;; Consumer

(defn partitions-for
  "Given a consumer and a Jackdaw topic descriptor, return the
  partitions assigned to the given consumer of the given topic as a
  collection of `TopicPartition`."
  [^Consumer consumer t]
  (.partitionsFor consumer (:jackdaw.topic/topic-name t)))

(defn offsets-for-times
  "Given a subscribed consumer, return a mapping from `TopicPartition`
  keys to `long` values being the offset into each partition of the
  FIRST record whose timestamp is equal to or greater than the given
  timestamp.

  Timestamps are longs to MS precision in UTC per convention.

  See https://kafka.apache.org/0101/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#offsetsForTimes(java.util.Map)"
  [^Consumer consumer partition-timestamps]
  (.offsetsForTimes consumer partition-timestamps))

(defn seek
  "Seek the consumer to the specified offset on the specified partition.

  Returns the consumer for convenience with `->`, `doto` etc.

  See https://kafka.apache.org/0101/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#seek(org.apache.kafka.common.TopicPartition,%20long)"
  [^Consumer consumer ^TopicPartition topic-partition ^long offset]
  (doto consumer
    (.seek topic-partition offset)))

;; Topic as sequence

(defn log-seq
  "Given a consumer, returns a lazy sequence of ConsumerRecords.

  If fuse-fn was provided, stops after fuse-fn returns false."
  ([^Consumer consumer polling-interval-ms]
   (log-seq consumer polling-interval-ms (constantly true)))
  ([^Consumer consumer polling-interval-ms fuse-fn]
   (let [r (jc/poll consumer polling-interval-ms)]
     (if (fuse-fn r)
       (do (log/debugf "Got %d records" (count r))
           (lazy-cat
            r
            (log-seq consumer polling-interval-ms fuse-fn)))
       r))))

(defn log-seq-until
  "Given a consumer, returns a lazy sequence of ConsumerRecords.

  Stops when current time > end-at"
  [^Consumer consumer polling-interval-ms end-at-ms]
  (log-seq consumer
           polling-interval-ms
           (fn [_]
             (< (System/currentTimeMillis)
                end-at-ms))))

(defn log-seq-until-inactivity
  "Given a consumer, returns a lazy sequence of ConsumerRecords.

  Stops when no messages are returned from poll"
  [^Consumer consumer polling-interval-ms]
  (log-seq consumer polling-interval-ms seq))

(def ->message (juxt :key :value))

(defn log-messages
  "Returns a lazy sequence of the keys and values of the messages from a Consumer.

  Stops consuming after consumer-timeout-ms."
  ([^Consumer consumer polling-interval-ms]
   (map ->message
        (log-seq consumer polling-interval-ms)))
  ([^Consumer consumer polling-interval-ms fuse-fn]
   (map ->message
        (log-seq consumer polling-interval-ms fuse-fn))))

;; Higher level API

(defn wait-for-inactivity
  "Waits until no messages have been produced on the topic in the given duration"
  [config topic inactivity-ms]
  (with-open [^Consumer consumer (jc/subscribed-consumer config topic)]
    (println "Skipped" (count (log-seq-until-inactivity consumer inactivity-ms)) "messages")))

(defn count-messages
  "Consumes all of the messages on a topic to determine the current count"
  [config topic]
  (with-open [^Consumer consumer (-> (jc/subscribed-consumer config topic)
                                     (jc/seek-to-beginning-eager))]
    (count (log-seq-until-inactivity consumer 2000))))

(defn- partition-info->topic-partition [^PartitionInfo x]
  (TopicPartition. (.topic x) (.partition x)))

(defn assign-all
  "Assigns all of the partitions for all of the given topics to the consumer"
  [^Consumer consumer topic & other-topics]
  (let [partitions (->> (conj other-topics topic)
                        (mapcat #(partitions-for consumer %))
                        (map partition-info->topic-partition))]
    (apply jc/assign consumer partitions)))

(defn seek-to-timestamp
  "Given an epoch ms timestamp a subscribed consumer and a seq of
  Jackdaw topics, seek all partitions of the selected topics to the
  offsets reported by Kafka to correlate with the given timestamp.

  After seeking, the first message read from each partition will be
  the EARLIEST message whose timestamp is greater than or equal to the
  timestamp sought.

  Returns the consumer for convenience with `->`, `doto` etc."
  [^Consumer consumer timestamp topics]
  (let [topic-partitions (->> (mapcat #(partitions-for consumer %) topics)
                              (map partition-info->topic-partition))
        start-offsets (offsets-for-times consumer (zipmap topic-partitions
                                                          (repeat timestamp)))]

    (doseq [[^TopicPartition topic-partition
             ^OffsetAndTimestamp timestamp-offset] start-offsets]
      ;; timestamp-offset is nil if the topic has no messages
      (let [offset (if timestamp-offset
                     (.offset timestamp-offset)
                     0)]
        (log/infof "Setting starting offset (topic=%s, partition=%s): %s"
                   (.topic topic-partition)
                   (.partition topic-partition)
                   offset)
        (seek consumer topic-partition offset)))

    consumer))

;; Caching

(defn add-shutdown-hook
  "Registers a runtime shutdown hook"
  [^Runnable f]
  (.addShutdownHook (Runtime/getRuntime) (Thread. f)))

;; Partitioning

(defn ^Integer default-partition*
  "Mimics the kafka default partitioner"
  [^bytes key-bytes ^Integer num-partitions]
  {:pre [(some? key-bytes) (pos? num-partitions)]}
  (-> key-bytes Utils/murmur2 Utils/toPositive (mod num-partitions) int))

(defn default-partition
  "Mimics the kafka default partitioner, given a message key"
  [{:keys [jackdaw.serdes/key-serde
           jackdaw.topic/topic-name
           jackdaw.topic/partitions]
    :as topic-config}
   key]
  (let [key-bytes (.serialize (.serializer ^Serde key-serde) topic-name key)]
    (default-partition* key-bytes partitions)))

;; This protocol interprets "record-key" and "partition-key"
;; into a particular function for extracting the message key
;; out of a message value
(defprotocol RecordKeyFn
  (record-key-fn* [record-key partition-key]))

(extend-protocol RecordKeyFn
  ;; Keywords are interpreted as themselves
  ;; unless the keyword is :partition-key,
  ;; in which case, the partition-key is interpreted.
  clojure.lang.Keyword
  (record-key-fn* [record-key partition-key]
    (if (= :partition-key record-key)
      (record-key-fn* partition-key nil)
      record-key))

  ;; Strings are coerced from "json path" to keywords for interpretation.
  String
  (record-key-fn* [record-key partition-key]
    {:pre [(clojure.string/starts-with? record-key "$.")]}
    (let [record-key* (-> record-key
                          (clojure.string/replace  "$." "")
                          (clojure.string/replace  "_" "-")
                          (clojure.string/split  #"\."))]
      (record-key-fn* (vec (map keyword record-key*)) partition-key)))

  ;; Functions are interpreted as themselves.
  clojure.lang.AFn
  (record-key-fn* [record-key partition-key]
    record-key))

(defn- record-key-fn
  "Gets a record key extraction function for the given topic"
  [{:keys [:jackdaw.topic/record-key :jackdaw.topic/partition-key] :as topic}]
  (record-key-fn* record-key partition-key))

(defn value-record-key
  "Gets the record key for a given topic and message"
  [{:keys [:jackdaw.topic/record-key :jackdaw.topic/partition-key] :as topic}
   value]
  (let [key-fn (record-key-fn topic)]
    (if (vector? key-fn)
      (get-in value key-fn)
      (key-fn value))))

(defn- record-partition
  "Gets the correct partition for a given topic and message"
  [{:keys [:jackdaw.topic/record-key ::serdes/key-serde] :as topic}
   value
   & [{:keys [key] :as opts}]]
  {:pre [(= :partition-key record-key)]}
  (let [key* (or key (value-record-key topic value))]
    (default-partition topic key*)))

(defn record-map
  "Creates an 'envelope' including the key and partition for a given topic and value"
  [{:keys [:jackdaw.topic/topic-name ::serdes/key-serde] :as topic}
   value
   & [{:keys [key partition] :as opts}]]
  (let [key* (or key (value-record-key topic value))
        partition* (or partition (record-partition topic value {:key key}))]
    (jc/producer-record topic
                        (int partition*)
                        key*
                        value)))

(defn kafka-stream-running? [^KafkaStreams kstream]
  (.isRunning (.state kstream)))

(defn kafka-stream-state-fn [^KafkaStreams kstream]
  (fn []
    (str (.state kstream))))
