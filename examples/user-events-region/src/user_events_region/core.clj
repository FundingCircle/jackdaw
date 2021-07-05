(ns user-events-region.core
  "Demonstrates how to perform a join between a KStream and a KTable.

   We join a stream of click events that reads from the click-events topic
   and users table that reads from a topic named user-region to compute the
   number of clicks per region."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.pprint :as pp]
            [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn app-config []
  {"application.id"            "region-clicks"
   "bootstrap.servers"         "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn topic-names []
  ["user-region" "click-events" "clicks-per-region"])

(defn user-region-entries []
  (vector ["alice" "asia"]
          ["bob" "america"]
          ["chao" "asia"]
          ["alice" "europe"]
          ["eve" "america"]
          ["fang" "asia"]
          ["gandalf" "europe"]
          ["marina" "europe"]))

(defn click-events-entries []
  (vector ["gandalf" 254]
          ["fang" 834]
          ["chao" 550]
          ["bob" 970]
          ["alice" 69]
          ["bob" 936]
          ["bob" 675]))

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
  [builder]
  (let [user-region-table (j/ktable builder (topic-config "user-region"))

        click-events-stream (j/kstream builder (topic-config "click-events"))

        ;; perform a left-join on regions-table ("alice" "us") and click events ("alice" 200)
        ;; and compute the number of clicks per region
        clicks-region-counts (-> click-events-stream
                                (j/left-join user-region-table
                                             (fn [events region] (vector region events)))
                                (j/map (fn [[_ v]] v))
                                (j/group-by-key (topic-config nil (Serdes/String) (Serdes/Long)))

                                ;; sums up the individual number of clicks by geo-region
                                (j/reduce (fn [acc curr] (+ acc curr))
                                          (topic-config "clicks-store")))]

    (-> clicks-region-counts
        (j/to-kstream)
        (j/peek (fn [[k v]] (println (str {:key k :value v}))))
        (j/to (topic-config "clicks-per-region")))
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

  (doseq [[key value] (user-region-entries)]
    (publish (topic-config "user-region") key value))

  (doseq [[key value] (click-events-entries)]
    (publish (topic-config "click-events") key value))

  (get-keyvals (topic-config "clicks-per-region"))
  ;; You should see the value of America updated from
  ;; ["america" 2581] to ["america" 7581]
  (publish (topic-config "click-events") "bob" 5000)

  (get-keyvals (topic-config "clicks-per-region"))

  )