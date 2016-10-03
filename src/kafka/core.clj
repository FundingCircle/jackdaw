(ns kafka.core
  "Clojure wrapper to kafka consumers/producers"
  (:require
   [clojure.tools.logging :as log])
  (:import
   (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
   (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback)
   (java.util.concurrent LinkedBlockingQueue)
   (clojure.lang Reflector)))

(defn producer
  "Return a KafkaProducer with the supplied properties"
  ([config]
   (KafkaProducer. config))

  ([config key-serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn consumer
  "Return a KafkaConsumer with the supplied properties"
  ([props]
   (KafkaConsumer. props))

  ([props key-deserializer value-deserializer]
   (KafkaConsumer. props key-deserializer value-deserializer)))

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
  "Clojurize the metadata returned from producing a kafka record"
  [record-meta]
  (select-methods record-meta
                  [:checksum :offset :partition
                   :serializedKeySize :serializedValueSize
                   :timestamp :topic :toString]))

(defn record [consumer-record]
  (select-methods consumer-record
                  [:checksum :key :offset :partition
                   :serializedKeySize :serializedValueSize
                   :timestamp :timestampType
                   :topic :toString :value]))
