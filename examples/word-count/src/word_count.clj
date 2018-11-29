(ns word-count
  "This tutorial contains a simple stream processing application using
  Jackdaw and Kafka Streams.

  Word Count reads from a Kafka topic called `input`, logs the key and
  value, and writes the counts to a topic called `output`. It uses a
  KTable to track how many times words are seen."
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jackdaw.streams :as j]
            [jackdaw.serdes.edn :as jse])
  (:import [org.apache.kafka.common.serialization Serdes]))


(defn topic-config
  "Takes a topic name and (optionally) a key and value serde and
  returns a topic configuration map, which may be used to create a
  topic or produce/consume records."
  ([topic-name]
   (topic-config topic-name (jse/serde) (jse/serde)))

  ([topic-name key-serde value-serde]
   {:topic-name topic-name
    :partition-count 1
    :replication-factor 1
    :topic-config {}
    :key-serde key-serde
    :value-serde value-serde}))


(defn app-config
  "Returns the application config."
  []
  {"application.id" "word-count"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  "Reads from a Kafka topic called `input`, logs the key and value,
  and writes the counts to a topic called `output`. Returns a topology
  builder."
  [builder]
  (let [text-input (-> (j/kstream builder (topic-config "input"))
                       (j/peek (fn [[k v]]
                                 (info (str {:key k :value v})))))

        counts (-> text-input
                   (j/flat-map-values (fn [v]
                                        (str/split (str/lower-case v)
                                                   #"\W+")))
                   (j/group-by (fn [[_ v]] v)
                               (topic-config nil (Serdes/String)
                                             (Serdes/String)))
                   (j/count))]

    (-> counts
        (j/to-kstream)
        (j/to (topic-config "output")))

    builder))

(defn start-app
  "Starts the stream processing application."
  [app-config]
  (let [builder (j/streams-builder)
        topology (build-topology builder)
        app (j/kafka-streams topology app-config)]
    (j/start app)
    (info "word-count is up")
    app))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (info "word-count is down"))


(defn -main
  [& _]
  (start-app (app-config)))


(comment
  ;;; Evaluate the forms.
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


  ;; Write to the input stream.
  (publish (topic-config "input") nil "all streams lead to kafka")
  (publish (topic-config "input") nil "hello kafka streams")


  ;; Read from the output stream.
  (get-keyvals (topic-config "output"))


  )
