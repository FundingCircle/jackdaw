(ns user
  "doc-string"
  (:require [clojure.java.shell :refer [sh]]
            [jackdaw.client :as jc]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :as jse]
            [jackdaw.streams :as j]
            [confluent]
            [system])
  (:import org.apache.kafka.common.serialization.Serdes))


;;; ------------------------------------------------------------
;;;
;;; Configure topics
;;;

(defn topic-config
  "Takes a topic name and (optionally) key and value serdes and a
  partition count, and returns a topic configuration map, which may be
  used to create a topic or produce/consume records."
  ([topic-name]
   (topic-config topic-name (jse/serde)))

  ([topic-name value-serde]
   (topic-config topic-name (jse/serde) value-serde))

  ([topic-name key-serde value-serde]
   (topic-config topic-name 1 key-serde value-serde))

  ([topic-name partition-count key-serde value-serde]
   {:topic-name topic-name
    :partition-count partition-count
    :replication-factor 1
    :topic-config {}
    :key-serde key-serde
    :value-serde value-serde}))


;;; ------------------------------------------------------------
;;;
;;; Create and list topics
;;;

(defn kafka-admin-client-config
  []
  {"bootstrap.servers" "localhost:9092"})

(defn create-topic
  "Takes a topic config and creates a Kafka topic."
  [topic-config]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/create-topics! client [topic-config])))

(defn list-topics
  "Returns a list of Kafka topics."
  []
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/list-topics client)))

(defn topic-exists?
  "Takes a topic name and returns true if the topic exists."
  [topic-config]
  (with-open [client (ja/->AdminClient (kafka-admin-client-config))]
    (ja/topic-exists? client topic-config)))


;;; ------------------------------------------------------------
;;;
;;; Produce and consume records
;;;

(defn kafka-producer-config
  []
  {"bootstrap.servers" "localhost:9092"})

(defn kafka-consumer-config
  []
  {"bootstrap.servers" "localhost:9092"
   "group.id" "jackdaw-user-consumer"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false"})


(defn publish
  "Takes a topic config and record value, and (optionally) a key and
  parition number, and produces to a Kafka topic."
  ([topic-config value]
   (with-open [client (jc/producer (kafka-producer-config) topic-config)]
     @(jc/produce! client topic-config value))
   nil)

  ([topic-config key value]
   (with-open [client (jc/producer (kafka-producer-config) topic-config)]
     @(jc/produce! client topic-config key value))
   nil)

  ([topic-config partition key value]
   (with-open [client (jc/producer (kafka-producer-config) topic-config)]
     @(jc/produce! client topic-config partition key value))
   nil))


(defn get-records
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of maps."
  ([topic-config]
   (get-records topic-config 30))

  ([topic-config timeout]
   (with-open [client (jc/subscribed-consumer (kafka-consumer-config)
                                              [topic-config])]
     (jc/poll client timeout))))


(defn get-keyvals
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of key-value pairs."
  ([topic-config]
   (get-keyvals topic-config 30))

  ([topic-config timeout]
   (map (juxt :key :value) (get-records topic-config timeout))))


;;; ------------------------------------------------------------
;;;
;;; Start and stop the system
;;;

(defn start
  "Creates topics, and starts the app."
  []
  (alter-var-root #'system/system merge (system/start)))

(defn stop
  "Stops the app, and deletes topics and internal state."
  []
  (system/stop)
  (alter-var-root #'system/system (constantly nil)))

(defn reset
  "Resets the app."
  []
  (stop)
  (start))
