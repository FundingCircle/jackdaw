(ns jackdaw.serdes.avro2.uuid
  (:require [jackdaw.serdes.avro2 :as avro])
  (:import (java.util UUID)))

(defrecord StringUUIDType []
  avro/SchemaType
  (avro->clj [_ uuid-str]
    (assert (string? uuid-str))
    (UUID/fromString uuid-str))
  (clj->avro [_ uuid]
    (assert (instance? UUID uuid))
    (str uuid)))

(defmethod avro/schema-type
  {:type "string" :logical-type "jackdaw.serdes.avro.UUID"}
  [_]
  (StringUUIDType.))
