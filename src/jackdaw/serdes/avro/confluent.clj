(ns jackdaw.serdes.avro.confluent
  (:require [jackdaw.serdes.avro :as jsa]))

(defn serde
  "Creates a serde for Avro data. An avro serde needs to know if its generating
  a key or value in the data (a separate serde is required for each as a result).
  Optionally accepts a map of further values to allow configuration of the avro
  serde operation."
  [schema-registry-url schema key? & [{:keys [type-registry
                                              schema-registry-client
                                              coercion-cache]}]]
  (let [reg (if (nil? type-registry)
              jsa/+base-schema-type-registry+
              type-registry)]
    (jsa/serde reg
               {:avro.schema-registry/url schema-registry-url
                :avro.schema-registry/client schema-registry-client}
               {:avro/schema schema
                :key? key?
                :avro/coercion-cache coercion-cache})))
