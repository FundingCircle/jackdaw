(ns kafka.streams.topology
  (:require
   [clojure.tools.logging :as log]
   [clojurewerkz.propertied.properties :as p]
   [kafka.streams.interop :as cljk]
   [kafka.streams :as k]))

(defn start-topologies
  "Starts a kafka stream from a list of topology builder functions. A topology
  builder function takes a TopologyBuilder and makes a kafka stream."
  ([topology-builders kafka-configs]
   (start-topologies topology-builders kafka-configs nil))
  ([topology-builders kafka-configs config]
   (try
     (let [topology-builder (cljk/topology-builder config)]
       (doseq [make-topology! topology-builders]
         (log/info "Making topology" {:topology make-topology!})
         (make-topology! topology-builder config))
       (let [stream (k/kafka-streams topology-builder
                                     (p/map->properties kafka-configs))]
         (k/start! stream)
         stream)))))
