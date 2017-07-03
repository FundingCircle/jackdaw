(ns jackdaw.serdes.edn
  (:require [clojure.edn :as edn]
            [taoensso.nippy :as nippy])
  (:import
   [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(defn edn-serializer
  "EDN serializer."
  []
  (reify Serializer
    (close [this])
    (configure [this configs key?])
    (serialize [this _topic data]
      (when data
        (nippy/freeze data)))))

(defn edn-deserializer
  "EDN deserializer."
  []
  (reify Deserializer
    (close [this])
    (configure [this configs key?])
    (deserialize [this _topic data]
      (when data
        (nippy/thaw data)))))

(defn edn-serde
  "EDN serde."
  []
  (Serdes/serdeFrom (edn-serializer) (edn-deserializer)))
