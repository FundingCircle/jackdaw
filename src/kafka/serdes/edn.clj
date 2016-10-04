(ns kafka.serdes.edn
  (:require [clojure.edn :as edn])
  (:import java.nio.charset.StandardCharsets
           [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(def edn-serializer
  (reify Serializer
    (close [this])
    (configure [this configs key?])
    (serialize [this _topic data]
      (when data
        (.getBytes (pr-str data))))))

(def edn-deserializer
  (reify Deserializer
    (close [this])
    (configure [this configs key?])
    (deserialize [this _topic data]
      (when data
        (edn/read-string (String. data StandardCharsets/UTF_8))))))

(def edn-serde
  (Serdes/serdeFrom edn-serializer edn-deserializer))
