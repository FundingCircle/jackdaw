(ns jackdaw.serdes.edn
  "FIXME"
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.edn]
            [jackdaw.serdes.fn :as j.s.fn])
  (:import java.nio.charset.StandardCharsets
           org.apache.kafka.common.serialization.Serdes))

(set! *warn-on-reflection* true)

(defn to-bytes
  "Convert a string to byte array."
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(defn from-bytes
  "Converts a byte array to a string."
  [^bytes data]
  (String. data StandardCharsets/UTF_8))

(defn serializer
  "Returns an EDN serializer."
  []
  (j.s.fn/new-serializer {:serialize (fn [_ _ data]
                                       (when data
                                         (to-bytes (prn-str data))))}))

(defn deserializer
  "Returns an EDN deserializer."
  []
  (j.s.fn/new-deserializer {:deserialize (fn [_ _ data]
                                           (-> (from-bytes data)
                                               clojure.edn/read-string))}))

(defn serde
  "Returns EDN serde."
  []
  (Serdes/serdeFrom (serializer) (deserializer)))
