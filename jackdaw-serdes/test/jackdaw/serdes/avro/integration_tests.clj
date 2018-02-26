(ns jackdaw.serdes.avro.integration-tests
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro :as avro]
            [clojure.data.json :as json]
            [clj-uuid :as uuid]
            [clojure.java.io :as io]
            [environ.core :as env]
            [jackdaw.serdes.registry :as registry])
  (:import (org.apache.avro Schema$Parser)
           (org.apache.avro.generic GenericData$Record)
           (org.apache.kafka.common.serialization Serde)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)))

(def topic-config
  {:avro/schema (slurp (io/resource "resources/example_schema.avsc"))
   :schema.registry/url "http://localhost:8081"})

(defn with-real-client [config]
  (assoc config :schema.registry/client (registry/client config 5)))

(defn with-mock-client [config]
  (assoc config :schema.registry/client (MockSchemaRegistryClient.)))

(deftest mock-schema-registry
  (testing "schema can be serialized by registry client"
    (let [config (avro/serde-config :value (with-mock-client topic-config))
          serde ^Serde (avro/avro-serde config)]
      (let [msg {:customer-id (uuid/v4)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

(deftest ^:integration real-schema-registry
  (testing "schema registry set in environment"
    (with-redefs [env/env {:schema-registry-url "http://localhost:8081"}]
      (let [config (avro/serde-config :value (with-real-client topic-config))
            serde ^Serde (avro/avro-serde config)]
        (let [msg {:customer-id (uuid/v4)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg)))))))

  (testing "schema registry set in config"
    (with-redefs [env/env {:schema-registry-url "http://registry.example.com:8081"}]
      (let [config (avro/serde-config :value (with-real-client topic-config))
            serde ^Serde (avro/avro-serde config)]
        (let [msg {:customer-id (uuid/v4)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg))))))))
