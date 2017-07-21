(ns jackdaw.serdes.avro-test
  "Tests for Avro serialization/deserialization functionality."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro-schema :as avro-schema]
            [jackdaw.serdes.avro2 :as avro2]
            [jackdaw.serdes.avro2.uuid])
  (:import
   (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)
   (clojure.lang ExceptionInfo)
   (org.apache.kafka.common.serialization Serde)))


(def schema (slurp (io/resource "resources/example_schema.avsc")))

(def data (edn/read-string (slurp (io/resource "resources/example_record.edn"))))

(def topic-name "Test topic name" "test.topic")

(deftest serializer-deserializer-test
  (testing "Serializing and deserializing a map returns the same map"
    (let [client (MockSchemaRegistryClient.)
          url "http://localhost:8081"
          config (avro2/serde-config :value {:avro/schema schema
                                             :schema.registry/client client
                                             :schema.registry/url url})
          serde ^Serde (avro2/avro-serde config)
          ser (.serializer serde)
          de (.deserializer serde)]
      (is (= data (->> (.serialize ser topic-name data)
                       (.deserialize de topic-name)))))))

