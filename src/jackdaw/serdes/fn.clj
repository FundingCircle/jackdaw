(ns jackdaw.serdes.fn
  "FIXME"
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:require [clojure.spec.alpha :as s]
            [jackdaw.serdes.fn-impl :as fn-impl])
  (:import [org.apache.kafka.common.serialization Deserializer Serializer]))

(set! *warn-on-reflection* true)

(s/def ::serialize fn?)
(s/def ::close fn?)
(s/def ::configure fn?)

(s/fdef new-serializer :args (s/cat :args (s/keys :req-un [::serialize]
                                                  :opt-un [::close
                                                           ::configure])))

(defn new-serializer ^Serializer [args]
  (fn-impl/map->FnSerializer args))

(s/def ::deserialize fn?)
(s/fdef new-deserializer :args (s/cat :args (s/keys :req-un [::deserialize]
                                                    :opt-un [::close
                                                             ::configure])))

(defn new-deserializer ^Deserializer [args]
  (fn-impl/map->FnDeserializer args))

