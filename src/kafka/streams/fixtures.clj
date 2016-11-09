(ns kafka.streams.fixtures
  (:require [kafka.streams.topology :refer [start-topologies]]))

(defn kstreams-fixture
  "Returns a fixture that creates the topologies and starts stream processing."
  [topology-builders kafka-configs]
  (fn [t]
    (with-open [streams (start-topologies topology-builders kafka-configs)]
      (t))))
