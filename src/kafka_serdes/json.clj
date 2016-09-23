(ns kafka-serdes.json
  "Implements JSON serializer, deserializer, and SerDe."
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [inflections.core :as inflections])
  (:import
   (java.nio.charset StandardCharsets)
   (org.apache.kafka.common.serialization Deserializer
                                          Serdes
                                          Serializer)))

(set! *warn-on-reflection* true)

(defn bytes-to-string
  "Convert a byte array to string."
  [data]
  (-> (io/input-stream data)
       (slurp)))

(defn string-to-bytes
  "Convert a string to byte array."
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(def json-serializer
  "Create a JSON serializer"
  (reify Serializer
    (close [this])
    (configure [this configs key?])
    (serialize [this _topic data]
      (-> (json/write-str data)
          (string-to-bytes)))))

(def json-deserializer
  "Create a JSON deserializer"
  (reify Deserializer
    (close [this])
    (configure [this configs key?])
    (deserialize [this _topic data]
      (-> (bytes-to-string data)
          (json/read-str :key-fn keyword)
          (inflections/hyphenate-keys)))))

(defn json-serde
  "Create a JSON serde."
  []
  (Serdes/serdeFrom json-serializer json-deserializer))
