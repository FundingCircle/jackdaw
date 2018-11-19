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

;; decide on the API for produce/consume in user and produce/consume for test-driver
;; and if these can be made the same
;; what sort of data structure should a 'record' take?
;; maybe it should always be a map but producing we only give it variable num args
;; to keep it flexible

;; consider changing 'topic-config' to 'topic' everywhere!!!!

;; returns a topic config with default partitions and replication factor 1 (needed to
;; create the topic)
;; and default EDN serdes for the key and value (needed to produce to the topic)

;; show producing to a different partition

;; create a separte ns for all the commands I plan to eval
;; show an example where we publish avro and consume avro using a schema registry


;; --------------------------------------------------------------------------------
;; topic helper

(defn topic-config
  "doc-string"
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


;; --------------------------------------------------------------------------------
;; functions to create and list topics

(defn kafka-admin-client-config
  []
  {"bootstrap.servers" "localhost:9092"})

(defn create-topic
  "doc-string"
  [topic-config]
  (with-open [client (j.admin.client/client (kafka-admin-client-config))]
    (j.admin.client/create-topic client topic-config)))

(defn get-topics
  "doc-string"
  []
  (with-open [client (j.admin.client/client (kafka-admin-client-config))]
    (j.admin.client/get-topics client)))

(defn topic-exists?
  "doc-string"
  [topic-name]
  (with-open [client (j.admin.client/client (kafka-admin-client-config))]
    (j.admin.client/topic-exists? client topic-name)))


;; --------------------------------------------------------------------------------
;; functions to produce and consume records

(defn kafka-producer-config
  []
  {"bootstrap.servers" "localhost:9092"})

(defn kafka-consumer-config
  []
  {"bootstrap.servers" "localhost:9092"
   "group.id" "jackdaw-user-consumer"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false"})


;; Usage:
;;
;; (produce (topic-config "foo") the-key the-value)
;; publishes a record to a topic 'foo' with key the-key and value the-value
(defn publish
  "doc-string"
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

;; Usage:
;;
;; (first (get-records (topic-config "foo")))
;; returns the 1st record on a topic
;;
;; (get-records (topic-config "foo")))
;; returns all the records on a topic
;;
;; (reverse (take 2 (get-records (topic-config "foo"))))
;; returns the last two records in descending order
;;
(defn get-records
  "doc-string"
  ([topic-config]
   (get-records topic-config 30))

  ([topic-config timeout]
   (with-open [client (j.client/subscribed-consumer (kafka-consumer-config)
                                                    topic-config)]
     (j.client/poll client timeout))))


(defn get-keyvals
  "doc-string"
  ([topic-config]
   (get-keyvals topic-config 30))

  ([topic-config timeout]
   (map #(vals (select-keys % [:key :value])) (get-records topic-config timeout))))


;; --------------------------------------------------------------------------------
;; functions to set up and tear down the system

(defn start
  []
  "doc-string"
  (alter-var-root #'system/system merge (system/start)))

(defn stop
  []
  "doc-string"
  (system/stop)
  (alter-var-root #'system/system (constantly nil)))

(defn reset
  []
  "doc-string"
  (stop)
  (start))
