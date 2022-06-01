(ns jackdaw.serdes.edn2
  "Implements an EDN SerDes (Serializer/Deserializer)."
  (:require [clojure.edn]
            [jackdaw.serdes.fn :as jsfn])
  (:import java.nio.charset.StandardCharsets)
  (:gen-class
   :implements [org.apache.kafka.common.serialization.Serde]
   :prefix "EdnSerde-"
   :name jackdaw.serdes.EdnSerde))

(set! *warn-on-reflection* true)

(defn to-bytes
  "Converts a string to a byte array."
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(defn- from-bytes
  "Converts a byte array to a string."
  [^bytes data]
  (String. data StandardCharsets/UTF_8))

(defn edn-serializer
  "Returns an EDN serializer."
  []
  (jsfn/new-serializer {:serialize (fn [_ _ data]
                                     (when data
                                       (to-bytes (pr-str data))))}))

(defn edn-deserializer
  "Returns an EDN deserializer."
  ([]
   (edn-deserializer {}))
  ([opts]
   (let [opts (into {} opts)]
     (jsfn/new-deserializer {:deserialize (fn [_ _ data]
                                            (->> (from-bytes data)
                                                 (clojure.edn/read-string opts)))}))))

(def EdnSerde-configure
  (constantly nil))

(defn EdnSerde-serializer
  [& _]
  (edn-serializer))

(defn EdnSerde-deserializer
  [& _]
  (edn-deserializer))
