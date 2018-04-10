(ns jackdaw.client
  "Clojure wrapper to kafka consumers/producers"
  (:require [clojure.walk :refer [stringify-keys]])
  (:import [org.apache.kafka.clients.consumer
            Consumer
            ConsumerRecord
            KafkaConsumer]
           [org.apache.kafka.clients.producer
            Callback
            KafkaProducer
            ProducerRecord
            RecordMetadata
            Producer]
           [java.util Properties List]
           org.apache.kafka.common.serialization.Serde
           org.apache.kafka.common.TopicPartition))

(defn map->properties [m]
  (let [props (Properties.)]
    (when m
      (.putAll props (stringify-keys m)))
    props))

(set! *warn-on-reflection* true)

(defn producer-record
  "Creates a kafka ProducerRecord for use with `send!`."
  ([{:keys [jackdaw.topic/topic-name]} value]
   (ProducerRecord. topic-name value))
  ([{:keys [jackdaw.topic/topic-name]} key value]
   (ProducerRecord. topic-name key value))
  ([{:keys [jackdaw.topic/topic-name]} partition key value]
   (ProducerRecord. topic-name partition key value))
  ([{:keys [jackdaw.topic/topic-name]} partition timestamp key value]
   (ProducerRecord. ^String topic-name ^Integer partition ^Long timestamp key value)))

(defn topic-partition
  "Return a TopicPartition"
  [{:keys [:jackdaw.topic/topic-name] :as topic-config} partition]
  (TopicPartition. topic-name (int partition)))

(defn ^KafkaProducer producer
  "Return a KafkaProducer with the supplied properties"
  ([config]
   (KafkaProducer. ^Properties (map->properties config)))
  ([config {:keys [jackdaw.serdes/key-serde jackdaw.serdes/value-serde]}]
   (KafkaProducer. ^Properties (map->properties config)
                   (.serializer ^Serde key-serde)
                   (.serializer ^Serde value-serde))))

(defn record-metadata
  "Clojurizes an org.apache.kafka.clients.producer.RecordMetadata."
  [^RecordMetadata record-metadata]
  (when record-metadata
    {:checksum (.checksum record-metadata)
     :offset (.offset record-metadata)
     :partition (.partition record-metadata)
     :serialized-key-size (.serializedKeySize record-metadata)
     :serialized-value-size (.serializedValueSize record-metadata)
     :timestamp (.timestamp record-metadata)
     :topic (.topic record-metadata)}))

(defn callback
  "Build a kafka producer callback function out of a normal clojure one
   The function should expect two parameters, the first being the record
   metadata, the second being an exception if there was one. The function
   should check for an exception and handle it appropriately."
  [on-completion]
  (reify Callback
    (onCompletion [this record-meta exception]
      (on-completion (record-metadata record-meta) exception))))

(defn send!
  "Asynchronously sends a record to a topic, returning a Future. A callback function
  can be optionally provided that should expect two parameters: a map of the
  record metadata, and an exception instance, if an error occurred."
  ([producer record]
   (.send ^Producer producer record))
  ([producer record callback-fn]
   (.send ^Producer producer record (callback callback-fn))))

(defn ^KafkaConsumer consumer
  "Return a Consumer with the supplied properties."
  ([config]
   (KafkaConsumer. ^Properties (map->properties config)))
  ([config {:keys [jackdaw.serdes/key-serde jackdaw.serdes/value-serde]}]
   (KafkaConsumer. ^Properties (map->properties config)
                   (when key-serde (.deserializer ^Serde key-serde))
                   (when value-serde (.deserializer ^Serde value-serde)))))

(defn ^KafkaConsumer subscribe
  "Subscribe a consumer to topics. Returns the consumer."
  [^KafkaConsumer consumer & topic-configs]
  (.subscribe consumer ^List (mapv :jackdaw.topic/topic-name topic-configs))
  consumer)

(defn subscription [^KafkaConsumer consumer]
  (.subscription consumer))

(defn assignment [^KafkaConsumer consumer]
  (.assignment consumer))

(defn assignment-for-every-sub? [consumer]
  {:post [(do (println "assignment for every sub?" (subscription consumer) (assignment consumer) %) true)]}
  (let [assignments (set (assignment consumer))]
    (every? (fn [s]
              (contains? assignments s)) (subscription consumer))))

(defn wait-for-assignments
  "Block until a consumer has assignments for each subscribed topic"
  [^KafkaConsumer consumer]
  (while (and (seq (subscription consumer))
              (not (assignment-for-every-sub? consumer)))
    (Thread/sleep 50)))

(defn ^KafkaConsumer subscribed-consumer
  "Returns a consumer that is subscribed to a single topic."
  [config topic-config]
  (-> (consumer config topic-config)
      (subscribe topic-config)))

(defn- consumer-record
  "Clojurize the ConsumerRecord returned from consuming a kafka record"
  [^ConsumerRecord consumer-record]
  (when consumer-record
    {:checksum (.checksum consumer-record)
     :key (.key consumer-record)
     :offset (.offset consumer-record)
     :partition (.partition consumer-record)
     :serializedKeySize (.serializedKeySize consumer-record)
     :serializedValueSize (.serializedValueSize consumer-record)
     :timestamp (.timestamp consumer-record)
     :topic (.topic consumer-record)
     :value (.value consumer-record)}))

(defn poll
  "Polls kafka for new messages."
  [^Consumer consumer timeout]
  (mapv consumer-record (.poll consumer timeout)))

(defn seek-to-end
  "Seek to the last offset for all assigned partitions"
  ([^Consumer consumer]
   (.poll consumer 0)
   (.seekToEnd consumer [])
   consumer)
  ([^Consumer consumer topic-partitions]
   (.poll consumer 0)
   (.seekToEnd consumer topic-partitions)
   consumer))

(defn seek-to-beginning
  "Seek to the last offset for the given topic/partitions"
  ([^Consumer consumer]
   (.poll consumer 0)
   (.seekToBeginning consumer [])
   consumer)
  ([^Consumer consumer topic-partitions]
   (.poll consumer 0)
   (.seekToBeginning consumer topic-partitions)
   consumer)
  )

(defn position
  "Get the offset of the next record that will be fetched"
  [^Consumer consumer ^TopicPartition topic-partition]
  (.position consumer topic-partition))

(defn assignment
  "Get the partitions currently assigned to this consumer"
  [^Consumer consumer]
  (set (.assignment consumer)))

(defn assign
  "Assign a consumer to specific partitions for specific topics. Returns the consumer."
  [^Consumer consumer & topic-partitions]
  (.assign consumer topic-partitions)
  consumer)
