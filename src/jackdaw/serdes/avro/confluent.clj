(ns jackdaw.serdes.avro.confluent
  (:require [jackdaw.serdes.avro :as jsa]))

(set! *warn-on-reflection* true)

(defn serde
  "Creates a serde for Avro data. An avro serde needs to know if its generating
  a key or value in the data (a separate serde is required for each as a result).
  Optionally accepts a map of further values to allow configuration of the avro
  serde operation."
  [schema-registry-url schema key? & [{:keys [type-registry
                                              schema-registry-client
                                              coercion-cache
                                              read-only?
                                              serializer-properties
                                              deserializer-properties]}]]
  (let [reg (if (nil? type-registry)
              jsa/+base-schema-type-registry+
              type-registry)]
    (jsa/serde reg
               {:avro.schema-registry/url schema-registry-url
                :avro.schema-registry/client schema-registry-client}
               {:avro/schema schema
                :key? key?
                :avro/coercion-cache coercion-cache
                :read-only? read-only?
                :serializer-properties serializer-properties
                :deserializer-properties deserializer-properties})))
