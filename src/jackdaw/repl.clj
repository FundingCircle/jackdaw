(ns jackdaw.repl
  (:require [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]))

;;; ------------------------------------------------------------
;;;
;;; Create and list topics
;;;

(def admin-client-config
  {"bootstrap.servers" "localhost:9092"})

(defn create-topic
  "Takes a topic config and creates a Kafka topic."
  [topic-config]
  (with-open [client (ja/->AdminClient admin-client-config)]
    (ja/create-topics! client [topic-config])))

(defn list-topics
  "Returns a list of Kafka topics."
  []
  (with-open [client (ja/->AdminClient admin-client-config)]
    (ja/list-topics client)))

(defn topic-exists?
  "Takes a topic name and returns true if the topic exists."
  [topic-config]
  (with-open [client (ja/->AdminClient admin-client-config)]
    (ja/topic-exists? client topic-config)))


;;; ------------------------------------------------------------
;;;
;;; Produce and consume records
;;;

(def producer-config
  {"bootstrap.servers" "localhost:9092"})

(def consumer-config
  {"bootstrap.servers" "localhost:9092"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false"})

(defn publish
  "Takes a topic config and record value, and (optionally) a key and
  parition number, and produces to a Kafka topic."
  ([topic-config value]
   (with-open [client (jc/producer producer-config topic-config)]
     @(jc/produce! client topic-config value))
   nil)

  ([topic-config key value]
   (with-open [client (jc/producer producer-config topic-config)]
     @(jc/produce! client topic-config key value))
   nil)

  ([topic-config partition key value]
   (with-open [client (jc/producer producer-config topic-config)]
     @(jc/produce! client topic-config partition key value))
   nil))

(defn get-records
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of maps."
  ([topic-config]
   (get-records topic-config 200))

  ([topic-config polling-interval-ms]
   (let [client-config (assoc consumer-config
                              "group.id"
                              (str (java.util.UUID/randomUUID)))]
     (with-open [client (jc/subscribed-consumer client-config [topic-config])]
       (doall (jcl/log client 100 seq))))))

(defn get-keyvals
  "Takes a topic config, consumes from a Kafka topic, and returns a
  seq of key-value pairs."
  ([topic-config]
   (get-keyvals topic-config 20))

  ([topic-config polling-interval-ms]
   (map (juxt :key :value) (get-records topic-config polling-interval-ms))))
