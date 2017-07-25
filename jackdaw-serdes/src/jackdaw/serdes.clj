(ns jackdaw.serdes
  "Some useful serdes."
  (:refer-clojure :exclude [resolve])
  (:require [environ.core :refer [env]]
            [jackdaw.serdes
             [avro :as avro]
             [edn :as edn]
             [json :as json]
             [uuid :as uuid]])
  (:import org.apache.kafka.common.serialization.Serdes))

(defmulti serde
  "Returns a serde."
  (fn [topic-config] (or (::type topic-config) topic-config)))

(defmethod serde ::avro-key
  [topic-config]
  (-> (avro/serde-config :key topic-config)
      (avro/avro-serde)))

(defmethod serde ::avro-value
  [topic-config]
  (-> (avro/serde-config :value topic-config)
      (avro/avro-serde)))

(defmethod serde ::edn
  [_]
  (edn/edn-serde))

(defmethod serde ::json
  [_]
  (json/json-serde))

(defmethod serde ::uuid
  [_]
  (uuid/uuid-serde))

(defmethod serde ::byte-array
  [_]
  (Serdes/ByteArray))

(defmethod serde ::byte-buffer
  [_]
  (Serdes/ByteBuffer))

(defmethod serde ::double
  [_]
  (Serdes/Double))

(defmethod serde ::integer
  [_]
  (Serdes/Integer))

(defmethod serde ::long
  [_]
  (Serdes/Long))

(defmethod serde ::string
  [_]
  (Serdes/String))

(defn resolve
  "Loads the serdes for a topic spec."
  [{:keys [jackdaw.topic/key-serde jackdaw.topic/key-schema
           jackdaw.topic/value-serde jackdaw.topic/value-schema] :as topic-config}]
  (assoc topic-config
         ::key-serde (serde (assoc topic-config
                                   ::type key-serde
                                   :avro/schema key-schema))
         ::value-serde (serde (assoc topic-config
                                     ::type value-serde
                                     :avro/schema value-schema))))
