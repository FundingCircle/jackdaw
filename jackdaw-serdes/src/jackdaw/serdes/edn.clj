(ns jackdaw.serdes.edn
  (:require [taoensso.nippy :as nippy]
            [jackdaw.serdes.fn :as sfn])
  (:import
   [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(defn edn-serializer
  "EDN serializer."
  []
  (sfn/->FnSerializer nippy/freeze))

(defn edn-deserializer
  "EDN deserializer."
  []
  (sfn/->FnDeserializer nippy/thaw))

(defn edn-serde
  "EDN serde."
  []
  (Serdes/serdeFrom (edn-serializer) (edn-deserializer)))
