(ns echo-stream
  (:gen-class)
  (:require [jackdaw.streams :as k]
            [jackdaw.serdes :as serde]))

(defn topic-config [topic-name]
  {:topic-name topic-name
   :key-serde (serde/serde :jackdaw.serdes/string)
   :value-serde (serde/serde :jackdaw.serdes/string)})

(defn kafka-config []
  {"application.id" "jackdaw-test"
   "client.id" "jackdaw-test-client"
   "cache.max.bytes.buffering" "0"
   "num.stream.threads" "5"
   "default.deserialization.exception.handler" "org.apache.kafka.streams.errors.LogAndFailExceptionHandler"
   "acks" "all"
   "max.in.flight.requests.per.connection" "1"
   "replication.factor" "1"
   "bootstrap.servers" "localhost:9092"})

(defn build-topology [builder]
  (-> (k/kstream builder (topic-config "input"))
      (k/peek (fn [[k v]] (println k " = " v)))
      (k/to! (topic-config "output")))
  builder)

(defn run-topology [topology]
  (k/start! (k/kafka-streams topology (kafka-config))))

(defn -main [& args]
  (let [builder (k/streams-builder)
        topology (build-topology builder)]
    (run-topology topology)
    (println "Running! ctrl-c to stop!")))
