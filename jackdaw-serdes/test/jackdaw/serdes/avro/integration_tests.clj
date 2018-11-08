(ns jackdaw.serdes.avro.integration-tests
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clj-uuid :as uuid]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro.schema-registry :as registry])
  (:import (org.apache.avro Schema$Parser)
           (org.apache.avro.generic GenericData$Record)
           (org.apache.kafka.common.serialization Serde)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)))

(def +topic-config+
  {:key? false
   :avro/schema (slurp (io/resource "resources/example_schema.avsc"))
   :avro.schema-registry/url "http://localhost:8081"})

(defn with-mock-client [config]
  (assoc config :avro.schema-registry/client (MockSchemaRegistryClient.)))

(deftest mock-schema-registry
  (testing "schema can be serialized by registry client"
    (let [serde ^Serde (avro/avro-serde (with-mock-client +topic-config+))]
      (let [msg {:customer-id (uuid/v4)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

(deftest ^:integration real-schema-registry
  (testing "schema registry set in config"
    (let [serde ^Serde (avro/avro-serde +topic-config+)]
      (let [msg {:customer-id (uuid/v4)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))
