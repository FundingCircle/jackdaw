(ns jackdaw.serdes.edn
  "Implements an EDN SerDes (Serializer/Deserializer)."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as log]
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
     (jsfn/new-deserializer
      {:deserialize (fn [_ topic bytes]
                      (let [string (from-bytes bytes)]
                        (try
                          (edn/read-string opts string)
                          (catch Exception e
                            (let [msg "Deserialization error"]
                              (log/error e (str msg " for " topic))
                              (throw (ex-info msg {:topic topic :data string} e)))))))}))))

(defn serde
  "Returns an EDN serde."
  [& [opts]]
  (Serdes/serdeFrom (serializer) (deserializer opts)))
