(ns jackdaw.specs
  "Specs for `jackdaw`"
  (:require [clojure.spec.alpha :as s]))

(set! *warn-on-reflection* true)

;; The basic topic

(s/def ::topic-name
  string?)

(s/def ::partition-fn
  fn?)

(s/def :jackdaw.topic/topic
  (s/keys :req-un [::topic-name]
          :opt-un [::partition-fn]))


;; The basic serde

(s/def ::serde-keyword qualified-keyword?)
(s/def ::schema string?)
(s/def ::schema-filename string?)
(s/def ::read-only? boolean?)
(s/def ::key? boolean?)

(s/def :jackdaw.serde/serde
  (s/keys :req-un [::serde-keyword]
          :opt-un [::schema
                   ::schema-filename
                   ::key?]))


;; Avro serde

(defn exactly-one-true?
  [& args]
  (= 1 (count (filter identity args))))

(s/def :jackdaw.serde/confluent-avro-serde
  (s/and
   (s/keys :req-un [::serde-keyword
                    ::key?
                    (or ::schema
                        ::schema-filename
                        ::read-only?)])
   #(exactly-one-true? (:schema %)
                       (:schema-filename %)
                       (:read-only? %))))


;; Topics as used by creation clients

(s/def ::partition-count pos-int?)
(s/def ::replication-factor pos-int?)
(s/def ::topic-config (s/map-of string? string?))

(s/def :jackdaw.creation-client/topic
  (s/keys :req-un [::topic-name
                   ::partition-count
                   ::replication-factor
                   ::topic-config]))

;; Topics as used by publishers and subscribers

(s/def ::key-serde :jackdaw.serde/serde)
(s/def ::value-serde :jackdaw.serde/serde)

(s/def :jackdaw.serde-client/topic
  (s/keys :req-un [::topic-name
                   ::key-serde
                   ::value-serde]
          :opt-un [::partition-fn]))


;; Topics where only serdes are needed

(s/def :jackdaw.serde-only-client/topic
  (s/keys :req-un [::key-serde ::value-serde]))
