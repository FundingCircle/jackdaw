(ns jackdaw.serdes.edn.nippy
  (:require [jackdaw.serdes.fn :as j.s.fn]
            [taoensso.nippy :as nippy])
  (:import org.apache.kafka.common.serialization.Serdes))

(defn serializer
  "Returns an EDN-nippy serializer."
  []
  (j.s.fn/new-serializer {:serialize (fn [_ _ data]
                                       (when data
                                         (nippy/freeze data)))}))

(defn deserializer
  "Returns an EDN-nippy deserializer."
  []
  (j.s.fn/new-deserializer {:deserialize (fn [_ _ data]
                                           (when data
                                             (nippy/thaw data)))}))

(defn serde
  "Returns an EDN-nippy serde"
  []
  (Serdes/serdeFrom (serializer) (deserializer)))
