(ns kafka.client
  "Clojure wrapper to kafka consumers/producers"
  (:require
   [clojure.tools.logging :as log]
   [kafka.config :as config])
  (:import
   (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
   (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback)
   org.apache.kafka.common.serialization.Serde
   (java.util.concurrent LinkedBlockingQueue)
   (clojure.lang Reflector)))

(set! *warn-on-reflection* true)

(defprotocol Producer
  (send! [this key value] "Sends a message to a kafka topic.")
  (close [this] "Closes the producer"))

(deftype TopicProducer [^KafkaProducer kafka-producer topic-name]
  Producer
  (send! [_ key value]
    (log/debug "Sending blocking message" {:topic-name topic-name :key key :value value :kafka-producer kafka-producer})
    @(if key
       (.send kafka-producer (ProducerRecord. topic-name key value))
       (.send kafka-producer (ProducerRecord. topic-name value))))
  (close [_]
    (.close kafka-producer)))

(defn producer
  "Return a KafkaProducer with the supplied properties"
  ([config topic-name]
   (TopicProducer.
    (KafkaProducer. ^java.util.Properties (config/properties config))
    topic-name))

  ([config ^Serde key-serde ^Serde value-serde topic-name]
   (TopicProducer.
    (KafkaProducer. ^java.util.Properties (config/properties config)
                   (.serializer key-serde)
                   (.serializer value-serde))
    topic-name)))

(defn consumer
  "Return a KafkaConsumer with the supplied properties"
  ^KafkaConsumer
  ([config]
   (KafkaConsumer. ^java.util.Properties (config/properties config)))

  ([config ^Serde key-serde ^Serde value-serde]
   (log/debug "Making consumer" {:key-serde key-serde :value-serde value-serde})
   (KafkaConsumer. ^java.util.Properties (config/properties config)
                   (.deserializer key-serde)
                   (.deserializer value-serde)))

  ([props key-serde value-serde topic-name]
   (let [kafka-consumer (consumer props key-serde value-serde)]
     (.subscribe ^KafkaConsumer kafka-consumer [topic-name])
     kafka-consumer)))

(defn poll
  "Fetches data from a consumer."
  [^KafkaConsumer kafka-consumer timeout-ms]
  (iterator-seq (.. kafka-consumer (poll timeout-ms) (iterator))))

(defn callback
  "Build a kafka producer callback function out of a normal clojure one
   The function should expect two parameters, the first being the record
   metadata, the second being an exception if there was one. The function
   should check for an exception and handle it appropriately."
  [on-completion]
  (reify Callback
    (onCompletion [this record-metadata exception]
      (on-completion record-metadata exception))))

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

(defn log-seq
  "Given a consumer, returns a lazy sequence of ConsumerRecords. Stops after
  consumer-timeout-ms."
  ([^KafkaConsumer kafka-consumer poll-timeout-ms]
   (concat (poll kafka-consumer poll-timeout-ms)
           (lazy-seq (poll kafka-consumer poll-timeout-ms))))
  ([^KafkaConsumer kafka-consumer poll-timeout-ms consumer-timeout-ms]
   (let [stop-at (+ (System/currentTimeMillis) consumer-timeout-ms)]
     (concat (poll kafka-consumer poll-timeout-ms)
             (lazy-seq
              (when (< (System/currentTimeMillis) stop-at)
                (poll kafka-consumer poll-timeout-ms)))))))

(defn log-records
  "Returns a lazy sequence of clojurized ConsumerRecords from a KafkaConsumer.
  Stops consuming after timeout-ms."
  ([^KafkaConsumer kafka-consumer poll-timeout-ms]
   (map record (log-seq kafka-consumer poll-timeout-ms)))
  ([^KafkaConsumer kafka-consumer poll-timeout-ms consumer-timeout-ms]
   (map record (log-seq kafka-consumer poll-timeout-ms consumer-timeout-ms))))

(defn log-messages
  "Returns a lazy sequence of the keys and values of the messages from a
  KafkaConsumer. Stops consuming after consumer-timeout-ms."
  ([^KafkaConsumer kafka-consumer poll-timeout-ms]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records kafka-consumer poll-timeout-ms)))
  ([^KafkaConsumer kafka-consumer poll-timeout-ms consumer-timeout-ms]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records kafka-consumer poll-timeout-ms consumer-timeout-ms))))
