(ns jackdaw.serdes.avro.integration-tests
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clj-uuid :as uuid]
            [jackdaw.serdes.avro :as avro]
            [jackdaw.serdes.avro.schema-registry :as reg])
  (:import (org.apache.avro Schema$Parser)
           (org.apache.avro.generic GenericData$Record)
           (org.apache.kafka.common.serialization Serde)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)))

(def +type-registry+
  (merge avro/+base-schema-type-registry+
         avro/+UUID-type-registry+))

(def +topic-config+
  {:key? false
   :avro/schema (slurp (io/resource "resources/example_schema.avsc"))})

(def +real-schema-registry+
  {:avro.schema-registry/url "http://localhost:8081"})

(def +mock-schema-registry+
  (merge +real-schema-registry+
        { :avro.schema-registry/client (MockSchemaRegistryClient.)}))

(deftest mock-schema-registry
  (testing "schema can be serialized by registry client"
    (let [serde ^Serde (avro/avro-serde +type-registry+ +mock-schema-registry+ +topic-config+)]
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
    (let [serde ^Serde (avro/avro-serde +type-registry+ +real-schema-registry+ +topic-config+)]
      (let [msg {:customer-id (uuid/v4)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))
