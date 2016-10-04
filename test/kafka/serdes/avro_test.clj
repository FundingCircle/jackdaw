(ns kafka.serdes.avro-test
  "Tests for Avro serialization/deserialization functionality."
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [kafka.serdes.avro :as avro]
            [kafka.serdes.avro-schema :as avro-schema])
  (:import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient))

(def schema (slurp "test/resources/example_schema.avsc"))

(def data (edn/read-string (slurp "test/resources/example_record.edn")))

(def topic-name "Test topic name" "test.topic")

(deftest serializer-deserializer-test
  (testing "Serializing and deserializing a map returns the same map"
    (let [client (MockSchemaRegistryClient.)
          ser (avro/avro-serializer client schema "http://localhost" true)
          de (avro/avro-deserializer client "http://localhost" true)]
      (is (= data
             (->> (.serialize ser topic-name data)
                  (.deserialize de topic-name)
                  (avro-schema/generic-record->map)))))))
