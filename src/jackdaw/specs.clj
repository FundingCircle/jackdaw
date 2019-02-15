(ns jackdaw.specs
  "Specs for `jackdaw`"
  (:require [clojure.spec.alpha :as s]))

;; The basic topic

(s/def ::topic-name
  string?)

(s/def :jackdaw.topic/topic
  (s/keys :req-un [::topic-name]))

;; Topics as used by the clients (streams, client)

(s/def ::serde any?)
(s/def ::key-serde ::serde)
(s/def ::value-serde ::serde)

(s/def :jackdaw.serialization-clients/topic
  (s/keys :req-un [::topic-name
                   ::key-serde
                   ::value-serde]))

;; Topics as needed for creation

(s/def ::partition-count pos-int?)
(s/def ::replication-factor integer?)
(s/def ::topic-config (s/map-of string? string?))

(s/def :jackdaw.creation-clients/topic
  (s/keys :req-un [::topic-name
                   ::partition-count
                   ::replication-factor
                   ::topic-config]))
