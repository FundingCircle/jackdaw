(ns user
  "doc-string"
  (:require [clojure.java.shell :refer [sh]]
            [jackdaw.client :as j.client]
            [jackdaw.admin.client :as j.admin.client]
            [jackdaw.serdes.edn :as j.s.edn]
            [jackdaw.streams :as j]
            [jackdaw.client.extras :as j.client.extras]
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
   (topic-config topic-name (j.s.edn/serde)))

  ([topic-name value-serde]
   (topic-config topic-name (j.s.edn/serde) value-serde))

  ([topic-name key-serde value-serde]
   (topic-config topic-name 1 key-serde value-serde))

  ([topic-name partitions key-serde value-serde]
   {:jackdaw.topic/topic-name topic-name
    :jackdaw.topic/partitions partitions
    :jackdaw.topic/replication-factor 1
    :jackdaw.serdes/key-serde key-serde
    :jackdaw.serdes/value-serde value-serde}))


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
  (with-open [client (j.admin.client/client (kafka-admin-client-config))]
    (j.admin.client/create-topic client topic-config)))

(defn get-topics
  "Returns a list of Kafka topics."
  []
  (with-open [client (j.admin.client/client (kafka-admin-client-config))]
    (j.admin.client/get-topics client)))

(defn topic-exists?
  "Takes a topic name and returns true if the topic exists."
  [topic-name]
  (with-open [client (j.admin.client/client (kafka-admin-client-config))]
    (j.admin.client/topic-exists? client topic-name)))


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
   (with-open [client (j.client/producer (kafka-producer-config) topic-config)]
     (j.client/send! client (j.client/producer-record
                             topic-config
                             value))
     nil))

  ([topic-config key value]
   (with-open [client (j.client/producer (kafka-producer-config) topic-config)]
     (j.client/send! client (j.client/producer-record topic-config
                                                      key
                                                      value))
     nil))

  ([topic-config partition key value]
   (with-open [client (j.client/producer (kafka-producer-config) topic-config)]
     (j.client/send! client (j.client/producer-record topic-config
                                                      (int partition)
                                                      key
                                                      value))
     nil)))


(defn get-records
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of maps."
  ([topic-config]
   (get-records topic-config 30))

  ([topic-config timeout]
   (with-open [client (j.client/subscribed-consumer (kafka-consumer-config)
                                                    topic-config)]
     (j.client/poll client timeout))))


(defn get-keyvals
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of key-value pairs."
  ([topic-config]
   (get-keyvals topic-config 30))

  ([topic-config timeout]
   (map #(vals (select-keys % [:key :value])) (get-records topic-config timeout))))


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
