(ns jackdaw.serdes.edn
  (:require [taoensso.nippy :as nippy]
            [jackdaw.serdes.fn :as sfn])
  (:import [org.apache.kafka.common.serialization Serdes]))

(defn edn-serializer
  "EDN serializer."
  []
  (sfn/new-serializer {:serialize (fn [_ _ data]
                                    (when data
                                      (nippy/freeze data)))}))

(defn edn-deserializer
  "EDN deserializer."
  []
  (sfn/new-deserializer {:deserialize (fn [_ _ data]
                                        (when data
                                          (nippy/thaw data)))}))

(defn edn-serde
  "EDN serde."
  []
  (Serdes/serdeFrom (edn-serializer) (edn-deserializer)))
