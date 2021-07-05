(ns user-region
  "Demonstrates group-by operations and aggregations on KTable.

  In this specific example we compute the user count per geo-region
  from a KTable that contains <user, region> information."
  (:gen-class)
  (:require [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse]
            [clojure.tools.logging :as log])
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn app-config
  "Returns the application config."
  []
  {"application.id"            "user-region"
   "bootstrap.servers"         "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn topic-names
  []
  ["user-region" "region-user-count"])

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
  (let [user-region-table (j/ktable builder (topic-config "user-region"))

        ;; Aggregate the user counts of by region
        region-count (-> user-region-table
                         (j/group-by (fn [[_ region]] [region region])
                                     (topic-config nil
                                                   (Serdes/String)
                                                   (Serdes/String)))
                         (j/count))]

    (-> region-count
        (j/to-kstream)
        (j/peek (fn [[k v]] (println (str {:key k :value v}))))
        (j/to (topic-config "region-user-count")))

    builder))

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (topology-builder builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (log/info "Done")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (log/info "Done"))

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

  (defn user-region-entries []
    [["alice" "asia"]
     ["bob" "america"]
     ["kim" "asia"]
     ["alice" "europe"]                                     ;; Note: Alice moved from Asia to Europe
     ["eve" "america"]
     ["zhang" "asia"]
     ["john" "europe"]
     ["marina" "europe"]])

  ;;input user entries with the user as key and region as value
  (doseq [[key value] (user-region-entries)]
    (publish (topic-config "user-region") key value))

  (publish (topic-config "user-region") "alice" "asia")

  (get-keyvals (topic-config "region-user-count"))
  ;; Get something like:
  ;;( ["asia" 1]
  ;;  ["america" 1]
  ;;  ["asia" 2]
  ;;  ["asia" 1] -> Captures the change of Alice moving to Asia
  ;;  ["europe" 1]
  ;;  ["america" 2]
  ;;  ["asia" 2]
  ;;  ["europe" 2]
  ;;  ["europe" 3])

  (publish (topic-config "user-region") "gianni" "asia")

  (get-keyvals (topic-config "region-user-count"))
  ;; you should now see the "asia" updated to 3 ["asia" 3]

  )



