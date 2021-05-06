(ns pipe
  "This tutorial contains a simple stream processing application using
  Jackdaw and Kafka Streams.

  Pipe reads from a Kafka topic called `input`, logs the key and
  value, and writes these to a Kafka topic called `output`."
  (:gen-class)
  (:require [jackdaw.serdes.edn :as jse]
            [jackdaw.streams :as j]))


(defn topic-config
  "Takes a topic name and returns a topic configuration map, which may
  be used to create a topic or produce/consume records."
  [topic-name]
  {:topic-name         topic-name
   :partition-count    1
   :replication-factor 1
   :key-serde          (jse/serde)
   :value-serde        (jse/serde)})


(def app-config
  {"application.id"            "pipe"
   "bootstrap.servers"         "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn build-topology
  "Reads from a Kafka topic called `input`, logs the key and value,
  and writes these to a Kafka topic called `output`. Returns a
  topology builder."
  [builder]
  (-> (j/kstream builder (topic-config "input"))
      (j/peek (fn [[k v]] (println (str {:key k :value v}))))
      (j/to (topic-config "output")))
  builder)

(defn start-app
  "Starts the stream processing application."
  ([] (start-app app-config))
  ([app-config]
   (let [builder (j/streams-builder)
         topology (build-topology builder)
         app (j/kafka-streams topology app-config)]
     (j/start app)
     (println "pipe is up")
     app)))

(defn stop-app
  "Stops the stream processing application."
  [app]
  (j/close app)
  (println "pipe is down"))


(defn -main
  [& _]
  (start-app app-config))
