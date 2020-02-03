(ns jackdaw.repl
  (:require [clojure.java.shell :refer [sh]]
            [jackdaw.admin :as ja]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.serdes :as js]
            [jackdaw.streams :as j])
  (:import [clojure.lang ILookup Associative]))


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


;;; ------------------------------------------------------------
;;;
;;; Helpers for REPL-driven development
;;;

(deftype FakeTopicMetadata []
  ILookup
  (valAt [this key]
    {:topic-name (name key)
     :partition-count 1
     :replication-factor 1
     :key-serde (js/edn-serde)
     :value-serde (js/edn-serde)})

  Associative
  (assoc [this key val]
    this))

(def topic-metadata
  "Treat this fake just like a map.

  When used with a 'getter', returns the topic metadata for the topic
  given with EDN serdes and a partition count of one."
  (FakeTopicMetadata.))

(defn re-delete-topics
  "Takes an instance of java.util.regex.Pattern and deletes all Kafka
  topics that match."
  [client-config re]
  (with-open [client (ja/->AdminClient client-config)]
    (let [topics-to-delete (->> (ja/list-topics client)
                                (filter #(re-find re (:topic-name %))))]
      (ja/delete-topics! client topics-to-delete))))

(defn destroy-state-stores
  "Takes an streams config and deletes local files associated with
  internal state."
  [streams-config]
  (sh "rm" "-rf" (str "/tmp/kafka-streams/"
                      (get streams-config "application.id"))))
