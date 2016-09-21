(ns kafka-serdes.avro-test
  "Tests for Avro serialization/deserialization functionality."
  (:require
   [clojure.edn :as edn]
   [clojure.test :refer :all]
   [clojure.test.check.clojure-test :as cct :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [kafka-serdes.avro :refer :all]
   [kafka-serdes.avro-schema :as avro])
  (:import
   (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)))

(def schema (slurp "test/resources/example_schema.avsc"))

(def data (edn/read-string (slurp "test/resources/example_record.edn")))

(def topic-name "Test topic name" "test.topic")

(deftest serializer-deserializer-test
  (testing "Serializing and deserializing a map returns the same map"
    (let [client (MockSchemaRegistryClient.)
          ser (avro-serializer client schema)
          de (avro-deserializer client)]
      (is (= data
             (->> (.serialize ser topic-name data)
                  (.deserialize de topic-name)
                  (avro/generic-record->map)))))))
