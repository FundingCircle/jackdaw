(ns jackdaw.serdes.json
  "Implements a JSON SerDes (Serializer/Deserializer)."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.data.json :as json]
            [jackdaw.serdes.fn :as jsfn])
  (:import java.nio.charset.StandardCharsets
           [org.apache.kafka.common.serialization.Serdes]))

(set! *warn-on-reflection* true)

(defn to-bytes
  "Converts a string to a byte array."
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(defn from-bytes
  "Converts a byte array to a string."
  [^bytes data]
  (String. data StandardCharsets/UTF_8))

(defn serializer
  "Returns a JSON serializer."
  []
  (jsfn/new-serializer {:serialize (fn [_ _ data]
                                     (when data
                                       (to-bytes (json/write-str data))))}))

(defn deserializer
  "Returns a JSON deserializer."
  []
  (jsfn/new-deserializer {:deserialize (fn [_ _ data]
                                         (when data
                                           (-> (from-bytes data)
                                               (json/read-str :key-fn keyword))))}))

(defn serde
  "Returns a JSON serde."
  []
  (Serdes/serdeFrom (serializer) (deserializer)))
