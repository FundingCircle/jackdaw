(ns jackdaw.serdes
  "Implements basic SerDes (Serializer/Deserializer)."
  (:gen-class)
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn ^{:deprecated "Use jackdaw.serdes.string/serde instead"} string-serde
  []
  (Serdes/String))
