(ns kafka.client
  "Clojure wrapper to kafka consumers/producers"
  (:require
   [clojure.tools.logging :as log]
   [clojurewerkz.propertied.properties :as p])
  (:import
   (clojure.lang Reflector)
   (org.apache.kafka.clients.consumer KafkaConsumer)
   (org.apache.kafka.clients.producer Callback
                                      KafkaProducer
                                      ProducerRecord)
   (org.apache.kafka.common TopicPartition)
   (org.apache.kafka.common.serialization Serde)
   (org.apache.kafka.common.utils Utils)))

(def default-fuse-timeout-ms 30000)
(def default-polling-interval-ms 1000)

(set! *warn-on-reflection* true)

;; https://github.com/apache/kafka/blob/41e676d29587042994a72baa5000a8861a075c8c/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L67
;; return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
(defn ^Integer default-partition*
  "Mimics the kafka default partitioner"
  [^bytes key-bytes ^Integer num-partitions]
  {:pre [(some? key-bytes) (pos? num-partitions)]}
  (-> key-bytes Utils/murmur2 Utils/toPositive (mod num-partitions) int))

(defn default-partition
  "Mimics the kafka default partitioner, given a message key"
  [{:keys [kafka.serdes/key-serde topic.metadata/name topic.metadata/partitions] :as topic-config}
   key]
  (let [key-bytes (.serialize (.serializer ^Serde key-serde) name key)]
    (default-partition* key-bytes partitions)))

(defn producer-record
  "Creates a kafka ProducerRecord for use with `send!`."
  ([{:keys [topic.metadata/name]} value] (ProducerRecord. name value))
  ([{:keys [topic.metadata/name]} key value] (ProducerRecord. name key value))
  ([{:keys [topic.metadata/name]} partition key value] (ProducerRecord. name partition key value))
  ([{:keys [topic.metadata/name]} partition timestamp key value] (ProducerRecord. name partition timestamp key value)))

(defn topic-partition
  "Return a TopicPartition"
  [{:keys [:topic.metadata/name] :as topic-config} partition]
  (TopicPartition. name (int partition)))

(defn producer
  "Return a KafkaProducer with the supplied properties"
  ([config]
   (log/debug "Making producer" {:config config})
   (KafkaProducer. ^java.util.Properties (p/map->properties config)))

  ([config {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
   (log/debug "Making producer" {:key-serde key-serde :value-serde value-serde})
   (KafkaProducer. ^java.util.Properties (p/map->properties config)
                   (.serializer ^Serde key-serde)
                   (.serializer ^Serde value-serde))))

(defn callback
  "Build a kafka producer callback function out of a normal clojure one
   The function should expect two parameters, the first being the record
   metadata, the second being an exception if there was one. The function
   should check for an exception and handle it appropriately."
  [on-completion]
  (reify Callback
    (onCompletion [this record-metadata exception]
      (on-completion record-metadata exception))))

(defn send!
  "Asynchronously sends a record to a topic."
  ([producer record]
   (.send ^KafkaProducer producer record))
  ([producer record callback-fn]
   (.send ^KafkaProducer producer record (callback callback-fn))))

(defn subscribe
  "Subscribe a consumer to topics. Returns the consumer."
  [consumer & topic-config]
  (.subscribe ^KafkaConsumer consumer (mapv :topic.metadata/name topic-config))
  consumer)

(defn assign
  "Assign a consumer to specific partitions for specific topics. Returns the consumer."
  [^KafkaConsumer consumer & topic-partitions]
  (.assign consumer topic-partitions)
  consumer)

(defn consumer
  "Return a KafkaConsumer with the supplied properties."
  ([config]
   (KafkaConsumer. ^java.util.Properties (p/map->properties config)))

  ([config {:keys [kafka.serdes/key-serde kafka.serdes/value-serde]}]
   (log/debug "Making consumer" {:config config
                                 :key-serde key-serde
                                 :value-serde value-serde})
   (KafkaConsumer. ^java.util.Properties (p/map->properties config)
                   (when key-serde (.deserializer ^Serde key-serde))
                   (when value-serde (.deserializer ^Serde value-serde)))))

(defn subscribe
  "Subscribe a consumer to topics. Returns the consumer."
  [consumer & topic-configs]
  (.subscribe ^KafkaConsumer consumer (mapv :topic.metadata/name topic-configs))
  consumer)

(defn position
  [^KafkaConsumer consumer topic-partition]
  (.position consumer topic-partition))

(defn- load-assignments
  "Forces the partitions to be assigned and returns them."
  [^KafkaConsumer consumer]
  (.poll consumer 0) ;; partition assignment occurs
  (.assignment consumer))

(defn seek-to-end
  "Seeks to the end of all the partitions assigned to the given consumer.
  Returns the consumer."
  [^KafkaConsumer consumer & topic-partitions]
  (.poll consumer 0)
  (let [assigned-partitions (or topic-partitions (load-assignments consumer))]
    (.seekToEnd consumer assigned-partitions)
    (doseq [assigned-partition assigned-partitions]
      ;; This forces the seek to happen now
      (.position consumer assigned-partition))
    consumer))

(defn consumer-subscription
  "Returns a consumer that is subscribed to a single topic."
  [config topic-config]
  (-> (consumer config topic-config)
      (subscribe topic-config)))

(defn select-methods
  "Like `select-keys` but instead builds a map by invoking the named java methods
   for the corresponding keys"
  [object methods]
  (let [jget (fn [m]
               (Reflector/invokeInstanceMethod object
                                               (str (name m)) (into-array [])))]
    (apply hash-map (->> methods
                         (map (fn [m]
                                [m (jget m)]))
                         (mapcat identity)))))

(defn metadata
  "Clojurize the ProducerRecord returned from producing a kafka record"
  [record-meta]
  (select-methods record-meta
                  [:checksum :offset :partition
                   :serializedKeySize :serializedValueSize
                   :timestamp :topic :toString]))

(defn record
  "Clojurize the ConsumerRecord returned from consuming a kafka record"
  [consumer-record]
  (select-methods consumer-record
                  [:checksum :key :offset :partition
                   :serializedKeySize :serializedValueSize
                   :timestamp :timestampType
                   :topic :toString :value]))

(defn next-records
  "Polls kafka until a message is received."
  [^KafkaConsumer consumer poll-timeout-ms fuse-fn]
  (when (fuse-fn)
    (or (.poll consumer poll-timeout-ms)
        (recur consumer poll-timeout-ms fuse-fn))))

(defn log-seq
  "Given a consumer, returns a lazy sequence of ConsumerRecords. Stops after
  fuse-fn returns false."
  ([^KafkaConsumer consumer polling-interval-ms]
   (log-seq consumer polling-interval-ms (constantly true)))
  ([^KafkaConsumer consumer polling-interval-ms fuse-fn]
   (let [records (next-records consumer polling-interval-ms fuse-fn)]
     (lazy-seq
      (concat records
              (log-seq consumer polling-interval-ms fuse-fn))))))

(defn log-records
  "Returns a lazy sequence of clojurized ConsumerRecords from a KafkaConsumer.
  Stops consuming after timeout-ms."
  ([^KafkaConsumer consumer polling-interval-ms]
   (map record (log-seq consumer polling-interval-ms)))
  ([^KafkaConsumer consumer polling-interval-ms fuse-fn]
   (map record (log-seq consumer polling-interval-ms fuse-fn))))

(defn log-messages
  "Returns a lazy sequence of the keys and values of the messages from a
  KafkaConsumer. Stops consuming after consumer-timeout-ms."
  ([^KafkaConsumer consumer polling-interval-ms]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records consumer polling-interval-ms)))
  ([^KafkaConsumer consumer polling-interval-ms fuse-fn]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records consumer polling-interval-ms fuse-fn))))

(defn timeout
  "Returns a function that throws an exception when called after some time has
  passed."
  ([]
   (timeout default-fuse-timeout-ms))
  ([millis]
   (let [end (+ millis (System/currentTimeMillis))]
     (fn []
       (if (< end (System/currentTimeMillis))
         (throw (ex-info "Timer expired" {:millis millis}))
         true)))))

(defn timed-log-messages
  "Same as `log-messages`, but will stop consuming after a specified timeout in
  milliseconds, or `default-fulse-time-ms` if no timeout is specified."
  ([^KafkaConsumer consumer]
   (log-messages consumer default-polling-interval-ms (timeout)))
  ([^KafkaConsumer consumer fuse-timeout-ms]
   (log-messages consumer default-polling-interval-ms (timeout fuse-timeout-ms))))
