(ns user-events.core
  "Demonstrates group-by operations and aggregations on KTable.

  In this specific example we compute the user count per geo-region
  from a KTable that contains <user, region> information."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn app-config
  "Returns the application config."
  []
  {"application.id"            "user-activity"
   "bootstrap.servers"         "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn topic-names
  []
  ["user-region" "region-user-count"])

(defn user-region-entries
  []
  (vector ["alice" "asia"]
          ["bob" "america"]
          ["chao" "asia"]
          ["alice" "europe"] ;; Note: Alice moved from Asia to Europe
          ["eve" "america"]
          ["fang" "asia"]
          ["gandalf" "europe"]
          ["marina" "europe"]))

(defn topic-config
  ([topic-name]
   (topic-config topic-name (jse/serde) (jse/serde)))
  ([topic-name key-serde value-serde]
   {:topic-name         topic-name
    :partition-count    1
    :replication-factor 1
    :topic-config       {}
    :key-serde          key-serde
    :value-serde        value-serde}))


(defn topology-builder
  "Reads from a Kafka topic called `user-region` and writes these to
  a Kafka topic called `region-user-count`. Returns a topology builder."
  [builder]
  (let [user-region-table
        (j/ktable builder (topic-config "user-region"))

        region-count (-> user-region-table
                         (j/group-by (fn [[_ v]] (vector v v))
                                     (topic-config nil
                                                   (Serdes/String)
                                                   (Serdes/String)))
                         (j/count))]

    ;; Aggregate the user counts of by region
    (-> region-count
        (j/to-kstream)
        (j/to (topic-config "region-user-count")))

    builder))

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (topology-builder builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "Done")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "Done"))

(defn -main
  "Starts the app"
  [& args]
  (start-app (app-config)))

(comment
  ;;; Start
  ;;;

  ;; Needed to invoke the forms from this namespace. When typing
  ;; directly in the REPL, skip this step.
  (require '[user :refer :all :exclude [topic-config]])


  ;; Start ZooKeeper and Kafka.
  ;; This requires the Confluent Platform CLI which may be obtained
  ;; from `https://www.confluent.io/download/`. If ZooKeeper and Kafka
  ;; are already running, skip this step.
  (confluent/start)


  ;; Create the `input` and `output` topics, and start the app.
  (start)


  ;; Get a list of current topics.
  (list-topics)

  ;;input user entries with the user as key and region as value
  (doseq [[key value] (user-region-entries)]
    (publish (topic-config "user-region") key value))

  (get-keyvals (topic-config "region-user-count"))
  ;( ["asia" 1]
  ;  ["america" 1]
  ;  ["asia" 2]
  ;  ["asia" 1] -> Captures the change of Alice moving to Asia
  ;  ["europe" 1]
  ;  ["america" 2]
  ;  ["asia" 2]
  ;  ["europe" 2]
  ;  ["europe" 3])

  )



