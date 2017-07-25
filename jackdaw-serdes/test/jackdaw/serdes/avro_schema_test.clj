(ns jackdaw.serdes.avro-schema-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [environ.core :as env]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro-schema :refer :all]
            [jackdaw.serdes.registry :as registry]
            [clj-uuid :as uuid])
  (:import
    (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient MockSchemaRegistryClient)
    (org.apache.avro Schema$Parser)
    (org.apache.avro.generic GenericData$Record GenericData$Array GenericData$EnumSymbol)
    (org.apache.avro.util Utf8)))
