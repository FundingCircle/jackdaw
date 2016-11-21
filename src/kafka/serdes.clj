(ns kafka.serdes
  "Some useful serdes."
  (:refer-clojure :exclude [resolve])
  (:require [environ.core :refer [env]]
            [kafka.serdes.avro :as avro]
            [kafka.serdes.json :as json])
  (:import org.apache.kafka.common.serialization.Serdes))

(defmulti serde
  "Returns a serde."
  (fn [config] (or (::type config) config)))

(defmethod serde ::avro-key
  [{json-schema :avro/schema}]
  (avro/avro-serde env json-schema true))

(defmethod serde ::avro-value
  [{json-schema :avro/schema}]
  (avro/avro-serde env json-schema false))

(defmethod serde ::json
  [_]
  (json/json-serde))

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
  [{:keys [topic.metadata/key-serde topic.metadata/key-schema
           topic.metadata/value-serde topic.metadata/value-schema] :as topic-config}]
  (assoc topic-config
         ::key-serde (serde {::type key-serde :avro/schema key-schema})
         ::value-serde (serde {::type value-serde :avro/schema value-schema})))
