(ns kafka.streams.topology
  (:require [kafka.streams.core :as k]
            kafka.config
            [clojure.tools.logging :as log]))

(defn start-topologies
  "Starts a kafka stream from a list of topology builder functions. A topology
  builder function takes a TopologyBuilder and makes a kafka stream."
  [topology-builders kafka-configs]
  (try
    (let [topology-builder (k/topology-builder)]
      (doseq [make-topology! topology-builders]
        (log/info "Making topology" {:topology make-topology!})
        (make-topology! topology-builder))
      (let [stream (k/kafka-streams topology-builder
                                    (kafka.config/properties kafka-configs))]
        (k/start! stream)
        stream))))
