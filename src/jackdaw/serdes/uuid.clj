(ns jackdaw.serdes.uuid
  "Implements UUID serializer, deserializer, and SerDe."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clj-uuid :as uuid]
            [jackdaw.serdes.fn :as sfn])
  (:import java.nio.ByteBuffer
           java.util.UUID
           [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))


(defn uuid-serializer
  "Create a UUID serializer."
  []
  (sfn/new-serializer {:serialize (fn [_ _ data]
                                    (when data
                                      (uuid/to-byte-array data)))}))

(defn uuid-deserializer
  "Create a UUID deserializer."
  []
  (sfn/new-deserializer {:deserialize (fn [_ _ data]
                                        (when data
                                          (let [bb (ByteBuffer/wrap data)]
                                            (UUID. (.getLong bb) (.getLong bb)))))}))

(defn uuid-serde
  "Create a UUID serde"
  []
  (Serdes/serdeFrom (uuid-serializer) (uuid-deserializer)))
