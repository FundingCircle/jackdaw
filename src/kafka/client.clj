(ns kafka.client
  "Clojure wrapper to kafka consumers/producers"
  (:require [clojure.tools.logging :as log]
            [kafka.config :as config])
  (:import clojure.lang.Reflector
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           org.apache.kafka.common.serialization.Serde))

(set! *warn-on-reflection* true)

(defn producer-record
  "Creates a kafka ProducerRecord for use with `send!`."
  ([topic-name value] (ProducerRecord. topic-name value))
  ([topic-name key value] (ProducerRecord. topic-name key value))
  ([topic-name partition key value] (ProducerRecord. topic-name partition key value))
  ([topic-name partition timestamp key value] (ProducerRecord. topic-name partition timestamp key value)))

(defn producer
  "Return a KafkaProducer with the supplied properties"
  ([config]
   (log/debug "Making producer" {:config config})
   (KafkaProducer. ^java.util.Properties (config/properties config)))

  ([config ^Serde key-serde ^Serde value-serde]
   (log/debug "Making producer" {:key-serde key-serde :value-serde value-serde})
   (KafkaProducer. ^java.util.Properties (config/properties config)
                   (.serializer key-serde)
                   (.serializer value-serde))))

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
   (.send ^KafkaProducer producer (callback callback-fn))))

(defn consumer
  "Return a KafkaConsumer with the supplied properties"
  ([config]
   (KafkaConsumer. ^java.util.Properties (config/properties config)))

  ([config ^Serde key-serde ^Serde value-serde]
   (log/debug "Making consumer" {:config config
                                 :key-serde key-serde
                                 :value-serde value-serde})
   (KafkaConsumer. ^java.util.Properties (config/properties config)
                   (.deserializer key-serde)
                   (.deserializer value-serde))))

(defn subscribe
  "Subscribe a consumer to topics. Returns the consumer."
  [consumer & topics]
  (.subscribe ^KafkaConsumer consumer topics)
  consumer)


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

(defn next-record
  "Polls kafka until a message is received."
  [^KafkaConsumer consumer poll-timeout-ms fuse-fn]
  (when (fuse-fn)
    (or (.poll consumer poll-timeout-ms)
        (recur consumer poll-timeout-ms fuse-fn))))

(defn log-seq
  "Given a consumer, returns a lazy sequence of ConsumerRecords. Stops after
  fuse-fn returns false."
  ([^KafkaConsumer consumer poll-timeout-ms]
   (log-seq consumer poll-timeout-ms (constantly true)))
  ([^KafkaConsumer consumer poll-timeout-ms fuse-fn]
   (lazy-seq
    (concat (next-record consumer poll-timeout-ms fuse-fn)
            (log-seq consumer poll-timeout-ms fuse-fn)))))

(defn log-records
  "Returns a lazy sequence of clojurized ConsumerRecords from a KafkaConsumer.
  Stops consuming after timeout-ms."
  ([^KafkaConsumer consumer poll-timeout-ms]
   (map record (log-seq consumer poll-timeout-ms)))
  ([^KafkaConsumer consumer poll-timeout-ms fuse-fn]
   (map record (log-seq consumer poll-timeout-ms fuse-fn))))

(defn log-messages
  "Returns a lazy sequence of the keys and values of the messages from a
  KafkaConsumer. Stops consuming after consumer-timeout-ms."
  ([^KafkaConsumer consumer poll-timeout-ms]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records consumer poll-timeout-ms)))
  ([^KafkaConsumer consumer poll-timeout-ms fuse-fn]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records consumer poll-timeout-ms fuse-fn))))
