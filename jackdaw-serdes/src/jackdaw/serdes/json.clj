(ns jackdaw.serdes.json
  "Implements JSON serializer, deserializer, and SerDe."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [jackdaw.serdes.fn :as sfn])
  (:import java.nio.charset.StandardCharsets
           [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(set! *warn-on-reflection* true)

(defn bytes-to-string
  "Convert a byte array to string."
  [^bytes data]
  (String. data StandardCharsets/UTF_8))

(defn string-to-bytes
  "Convert a string to byte array."
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(defn json-serializer
  "Create a JSON serializer"
  []
  (sfn/->FnSerializer #(-> %
                           json/write-str
                           string-to-bytes)))

(defn json-deserializer
  "Create a JSON deserializer"
  []
  (sfn/->FnDeserializer #(-> %
                           bytes-to-string
                           (json/read-str :key-fn keyword))))

(defn json-serde
  "Create a JSON serde."
  []
  (Serdes/serdeFrom (json-serializer) (json-deserializer)))
