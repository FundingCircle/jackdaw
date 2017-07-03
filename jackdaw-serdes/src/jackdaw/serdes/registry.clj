(ns jackdaw.serdes.registry
  (:require
   [environ.core :as env])
  (:import
   (io.confluent.kafka.schemaregistry.client
    MockSchemaRegistryClient
    CachedSchemaRegistryClient)))

(defn url
  [config]
  (or (get config :schema.registry/url)
      (env/env :schema-registry-url)))


(defn client
  [config max-capacity]
  (or (get config :schema.registry/client)
      (CachedSchemaRegistryClient. ^String (url config)
                                   ^int max-capacity)))

(defn mock-client
  []
  (MockSchemaRegistryClient.))
  
