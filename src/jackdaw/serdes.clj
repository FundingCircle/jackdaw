(ns jackdaw.serdes
  "Implements basic SerDes (Serializer/Deserializer)."
  (:gen-class)
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn string-serde
  []
  (Serdes/String))
