(ns jackdaw.serdes.uuid
  "Implements UUID serializer, deserializer, and SerDe."
  (:require [clj-uuid :as uuid]
            [jackdaw.serdes.fn :as sfn])
  (:import java.nio.ByteBuffer
           java.util.UUID
           [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))


(defn uuid-serializer
  "Create a UUID serializer."
  []
  (sfn/->FnSerializer uuid/to-byte-array))

(defn uuid-deserializer
  "Create a UUID deserializer."
  []
  (sfn/->FnDeserializer #(let [bb (ByteBuffer/wrap %)]
                           (UUID. (.getLong bb) (.getLong bb)))))

(defn uuid-serde
  "Create a UUID serde"
  []
  (Serdes/serdeFrom (uuid-serializer) (uuid-deserializer)))
