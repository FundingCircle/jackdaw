(ns kafka.serdes.registry
  (:require
   [environ.core :as env])
  (:import
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient)))

(defn url
  [config]
  (or (get config "schema-registry-url")
      (get config "schema.registry.url")
      (env/env :schema-registry-url)
      "http://localhost:8081"))

(defn client
  [config max-capacity]
  (CachedSchemaRegistryClient. ^String (url config)
                               ^int max-capacity))
