(ns jackdaw.serdes.edn
  "DEPRECATION NOTICE:

  This namespace is deprecated. Please use jackdaw.serdes/edn-serde.

  The behavior of the new EDN serde is different. It does not print
  the newline.

  Implements an EDN SerDes (Serializer/Deserializer)."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.edn]
            [jackdaw.serdes.fn :as jsfn])
  (:import java.nio.charset.StandardCharsets
           org.apache.kafka.common.serialization.Serdes))

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
  "Returns an EDN serializer."
  []
  (jsfn/new-serializer {:serialize (fn [_ _ data]
                                     (when data
                                       (to-bytes
                                        (binding [*print-length* false
                                                  *print-level* false]
                                          (prn-str data)))))}))

(defn deserializer
  "Returns an EDN deserializer."
  ([]
   (deserializer {}))
  ([opts]
   (let [opts (into {} opts)]
     (jsfn/new-deserializer {:deserialize (fn [_ _ data]
                                            (->> (from-bytes data)
                                                 (clojure.edn/read-string opts)))}))))

(defn serde
  "Returns an EDN serde."
  [& [opts]]
  (Serdes/serdeFrom (serializer) (deserializer opts)))
