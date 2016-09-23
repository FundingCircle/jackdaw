(ns kafka-serdes.serdes
  "Some useful serdes."
  (:require [environ.core :refer [env]]
            [kafka-serdes.avro :as avro]
            [kafka-serdes.json :as json])
  (:import org.apache.kafka.common.serialization.Serdes))

(defmulti load
  "Returns a serde."
  (fn [config] (or (:serde/type config) config)))

(defmethod load :serde/avro-key
  [{json-schema :avro/schema}]
  (avro/avro-serde env json-schema true))

(defmethod load :serde/avro-value
  [{json-schema :avro/schema}]
  (avro/avro-serde env json-schema false))

(defmethod load :serde/json
  [_]
  (json/json-serde))

(defmethod load :serde/byte-array
  [_]
  (Serdes/ByteArray))

(defmethod load :serde/byte-buffer
  [_]
  (Serdes/ByteBuffer))

(defmethod load :serde/double
  [_]
  (Serdes/Double))

(defmethod load :serde/integer
  [_]
  (Serdes/Integer))

(defmethod load :serde/long
  [_]
  (Serdes/Long))

(defmethod load :serde/string
  [_]
  (Serdes/String))

(defn load-serdes
  "Updates a topic configuration by instantiating the key and value serde."
  [topic-config]
  (-> topic-config
      (update :kafka-topic/key-serde load)
      (update :kafka-topic/value-serde load)))
