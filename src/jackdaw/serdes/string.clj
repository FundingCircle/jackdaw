(ns jackdaw.serdes.string
  (:import (org.apache.kafka.common.serialization Serdes)))

(defn serde
  []
  (Serdes/String))
