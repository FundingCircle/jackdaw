(ns jackdaw.serdes
  "Implements string and EDN serdes (serializer/deserializer).

  This is the public API for jackdaw.serdes."
  (:gen-class)
  (:require [jackdaw.serdes.edn2 :as jse]
            [jackdaw.serdes.fressian :as jsf])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn string-serde
  []
  (Serdes/String))

(defn edn-serde
  "Implements an EDN SerDes (Serializer/Deserializer).

  The behavior of this serde differs from the one in
  jackdaw.serdes.edn. It does not print a newline."
  [& [opts]]
  (Serdes/serdeFrom (jse/edn-serializer) (jse/edn-deserializer opts)))

(defn fressian-serde
  "Implements a Fressian SerDes (Serializer/Deserializer)."
  []
  (Serdes/serdeFrom (jsf/fressian-serializer) (jsf/fressian-deserializer)))
