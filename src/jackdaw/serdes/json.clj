(ns jackdaw.serdes.json
  "Implements JSON serializer, deserializer, and SerDe."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
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
  (sfn/new-serializer {:serialize (fn [_ _ data]
                                    (when data
                                      (-> data
                                          json/write-str
                                          string-to-bytes)))}))

(defn json-deserializer
  "Create a JSON deserializer"
  []
  (sfn/new-deserializer {:deserialize (fn [_ _ data]
                                        (when data
                                          (-> data
                                              bytes-to-string
                                              (json/read-str :key-fn keyword))))}))

(defn json-serde
  "Create a JSON serde."
  []
  (Serdes/serdeFrom (json-serializer) (json-deserializer)))
