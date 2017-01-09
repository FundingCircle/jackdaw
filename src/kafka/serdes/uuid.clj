(ns kafka.serdes.uuid
  "Implements UUID serializer, deserializer, and SerDe."
  (:require [clj-uuid :as uuid])
  (:import java.nio.ByteBuffer
           java.util.UUID
           [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(set! *warn-on-reflection* true)

(deftype UUIDSerializer []
  Serializer
  (close [this])
  (configure [this configs key?])
  (serialize [this _topic data]
    (when data
      (uuid/to-byte-array data))))

(deftype UUIDDeserializer []
  Deserializer
  (close [this])
  (configure [this configs key?])
  (deserialize [this _topic data]
    (when data
      (let [bb (ByteBuffer/wrap data)]
        (UUID. (.getLong bb) (.getLong bb))))))

(defn uuid-serializer
  "Create a UUID serializer."
  []
  (->UUIDSerializer))

(defn uuid-deserializer
  "Create a UUID deserializer."
  []
  (->UUIDDeserializer))

(defn uuid-serde
  "Create a UUID serde"
  []
  (Serdes/serdeFrom (uuid-serializer) (uuid-deserializer)))
