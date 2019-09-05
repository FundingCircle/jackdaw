(ns jackdaw.serdes
  "Implements string and EDN serdes (serializer/deserializer)."
  (:gen-class)
  (:require [clojure.edn]
            [jackdaw.serdes.fn :as jsfn])
  (:import java.nio.charset.StandardCharsets
           org.apache.kafka.common.serialization.Serde
           org.apache.kafka.common.serialization.Serdes))


(set! *warn-on-reflection* true)


(defn to-bytes
  "Converts a string to a byte array."
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(defn- from-bytes
  "Converts a byte array to a string."
  [^bytes data]
  (String. data StandardCharsets/UTF_8))


(defn string-serde
  []
  (Serdes/String))


(defn edn-serializer
  "Returns an EDN serializer."
  []
  (jsfn/new-serializer {:serialize (fn [_ _ data]
                                     (when data
                                       (to-bytes
                                        (binding [*print-length* false
                                                  *print-level* false]
                                          (pr-str data)))))}))

(defn edn-deserializer
  "Returns an EDN deserializer."
  ([]
   (edn-deserializer {}))
  ([opts]
   (let [opts (into {} opts)]
     (jsfn/new-deserializer {:deserialize (fn [_ _ data]
                                            (->> (from-bytes data)
                                                 (clojure.edn/read-string opts)))}))))

(defn edn-serde
  "Implements an EDN SerDes (Serializer/Deserializer).

  The behavior of this serde differs from the one in
  jackdaw.serdes.edn. It does not print a newline."
  [& [opts]]
  (Serdes/serdeFrom (edn-serializer) (edn-deserializer opts)))

(gen-class
    :name "jackdaw.serdes.EdnSerde"
	:implements [org.apache.kafka.common.serialization.Serde]
	:prefix "EdnSerde-")

(def EdnSerde-configure
  (constantly nil))

(defn EdnSerde-serializer
  [& _]
  (edn-serializer))

(defn EdnSerde-deserializer
  [& _]
  (edn-deserializer))
