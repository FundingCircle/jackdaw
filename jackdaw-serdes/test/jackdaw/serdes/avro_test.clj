(ns jackdaw.serdes.avro-test
  "Tests for Avro serialization/deserialization functionality."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro-schema :as avro-schema])
  (:import
   (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)
   (clojure.lang ExceptionInfo)))


(def schema (slurp (io/resource "resources/example_schema.avsc")))

(def data (edn/read-string (slurp (io/resource "resources/example_record.edn"))))

(def topic-name "Test topic name" "test.topic")

(deftest serializer-deserializer-test
  (testing "Serializing and deserializing a map returns the same map"
    (let [client (MockSchemaRegistryClient.)
          ser (avro/avro-serializer client schema {"schema.registry.url" "http://localhost:8081"} true)
          de (avro/avro-deserializer client schema {"schema.registry.url" "http://localhost:8081"} true)]
      (is (= data
             (->> (.serialize ser topic-name data)
                  (.deserialize de topic-name)))))))
