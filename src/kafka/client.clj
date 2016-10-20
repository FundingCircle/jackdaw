(ns kafka.client
  "Clojure wrapper to kafka consumers/producers"
  (:require [clojure.tools.logging :as log]
            [kafka.config :as config])
  (:import clojure.lang.Reflector
           java.lang.AutoCloseable
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord]
           org.apache.kafka.common.serialization.Serde))

(set! *warn-on-reflection* true)

(defprotocol Consumer
  (poll [this timeout]))

(defprotocol Producer
  (send! [this producer-record] "Sends a message to a kafka topic."))

(deftype TopicConsumer [^KafkaConsumer kafka-consumer topic-name]
  AutoCloseable
  (close [_]
    (.close kafka-consumer)
    (log/info "Closed kafka consumer" {:kafka-consumer kafka-consumer}))
  Consumer
  (poll [this timeout-ms]
    (log/debug "Polling kafka consumer" {:kafka-consumer kafka-consumer :timeout-ms timeout-ms})
    (iterator-seq (.. kafka-consumer (poll timeout-ms) (iterator)))))

(deftype TopicProducer [^KafkaProducer kafka-producer topic-name]
  AutoCloseable
  (close [_]
    (.close kafka-producer)
    (log/info "Closed kafka producer" {:kafka-producer kafka-producer}))
  Producer
  (send! [_ producer-record]
    (log/debug "Sending record" producer-record)
    (.send kafka-producer producer-record)))

(defn producer-record
  "Creates a kafka ProducerRecord for use with `send!`."
  ([topic-name value] (ProducerRecord. topic-name value))
  ([topic-name key value] (ProducerRecord. topic-name key value))
  ([topic-name partition key value] (ProducerRecord. topic-name partition key value))
  ([topic-name partition timestamp key value] (ProducerRecord. topic-name partition timestamp key value)))

(defn producer
  "Return a KafkaProducer with the supplied properties"
  ([config topic-name]
   (log/debug "Making producer" {:topic topic-name})
   (TopicProducer.
    (KafkaProducer. ^java.util.Properties (config/properties config))
    topic-name))

  ([config ^Serde key-serde ^Serde value-serde topic-name]
   (log/debug "Making producer" {:topic-name topic-name :key-serde key-serde :value-serde value-serde})
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

  ([config topic-name]
   (let [kafka-consumer (consumer config)]
     (.subscribe ^KafkaConsumer kafka-consumer [topic-name])
     (log/debug "Set subscription" {:topic topic-name})
     (TopicConsumer. kafka-consumer topic-name)))

  ([config ^Serde key-serde ^Serde value-serde]
   (log/debug "Making consumer" {:key-serde key-serde :value-serde value-serde})
   (KafkaConsumer. ^java.util.Properties (config/properties config)
                   (.deserializer key-serde)
                   (.deserializer value-serde)))

  ([config key-serde value-serde topic-name]
   (let [kafka-consumer (consumer config key-serde value-serde)]
     (.subscribe ^KafkaConsumer kafka-consumer [topic-name])
     (log/debug "Set subscription" {:topic topic-name})
     (TopicConsumer. kafka-consumer topic-name))))

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
  ([^TopicConsumer topic-consumer poll-timeout-ms]
   (concat (poll topic-consumer poll-timeout-ms)
           (lazy-seq
            (poll topic-consumer poll-timeout-ms))))
  ([^TopicConsumer topic-consumer poll-timeout-ms consumer-timeout-ms]
   (let [stop-at (+ (System/currentTimeMillis) consumer-timeout-ms)]
     (concat (poll topic-consumer poll-timeout-ms)
             (lazy-seq
              (when (< (System/currentTimeMillis) stop-at)
                (poll topic-consumer poll-timeout-ms)))))))

(defn log-records
  "Returns a lazy sequence of clojurized ConsumerRecords from a KafkaConsumer.
  Stops consuming after timeout-ms."
  ([^TopicConsumer topic-consumer poll-timeout-ms]
   (map record (log-seq topic-consumer poll-timeout-ms)))
  ([^TopicConsumer topic-consumer poll-timeout-ms consumer-timeout-ms]
   (map record (log-seq topic-consumer poll-timeout-ms consumer-timeout-ms))))

(defn log-messages
  "Returns a lazy sequence of the keys and values of the messages from a
  KafkaConsumer. Stops consuming after consumer-timeout-ms."
  ([^TopicConsumer topic-consumer poll-timeout-ms]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records topic-consumer poll-timeout-ms)))
  ([^TopicConsumer topic-consumer poll-timeout-ms consumer-timeout-ms]
   (map (fn [rec] [(:key rec) (:value rec)])
        (log-records topic-consumer poll-timeout-ms consumer-timeout-ms))))
