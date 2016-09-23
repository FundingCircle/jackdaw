(ns kafka-serdes.serdes
  "Some useful serdes."
  (:require [environ.core :refer [env]]
            [kafka-serdes.avro :as avro]
            [kafka-serdes.json :as json])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn byte-array-serde [] (Serdes/ByteArray))

(defn byte-buffer-serde [] (Serdes/ByteBuffer))

(defn double-serde [] (Serdes/Double))

(defn integer-serde [] (Serdes/Integer))

(defn long-serde [] (Serdes/Long))

(defn string-serde [] (Serdes/String))

(defn avro-value-serde
  ([]
   (avro-value-serde nil))
  ([json-schema]
   (avro/avro-serde env json-schema false)))

(defn avro-key-serde
  ([]
   (avro-key-serde nil))
  ([json-schema]
   (avro/avro-serde env json-schema true)))

(defn json-serde [] json/json-serde)
