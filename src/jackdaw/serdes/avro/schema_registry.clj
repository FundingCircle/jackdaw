(ns jackdaw.serdes.avro.schema-registry
  "Helpers for talking to one of Confluent's Avro schema registries."
  {:license "BSD 3-Clause License <https://github.com/FundingCircle/jackdaw/blob/master/LICENSE>"}
  (:import [io.confluent.kafka.schemaregistry.client
            MockSchemaRegistryClient
            CachedSchemaRegistryClient]))

(set! *warn-on-reflection* true)

(defn client
  "Build and return a Kafka Schema Registry client which uses an LRU
  strategy to cache the specified number of schemas."
  [^String url max-capacity]
  {:pre [(string? url)
         (pos-int? max-capacity)]}
  (CachedSchemaRegistryClient. url ^int max-capacity))

(defn mock-client
  "Build and return a mock schema registry client.

  Really suitable only for testing."
  []
  (MockSchemaRegistryClient.))
