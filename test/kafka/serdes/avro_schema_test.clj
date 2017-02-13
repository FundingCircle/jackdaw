(ns kafka.serdes.avro-schema-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [environ.core :as env]
            [kafka.serdes.avro :as avro]
            [kafka.serdes.avro-schema :refer :all]
            [kafka.serdes.registry :as registry])
  (:import
   (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient MockSchemaRegistryClient)))

(def schema (slurp (io/resource "resources/example_schema.avsc")))

(def record (edn/read-string (slurp (io/resource "resources/example_record.edn"))))

(defn with-mock-client [config]
  (assoc config
         :schema.registry/client (MockSchemaRegistryClient.)))

(defn with-real-client [config]
  (assoc config
         :schema.registry/client
         (registry/client config 5)))

(def avro-config
  {:avro/schema schema
   :avro/is-key false
   "schema.registry.url" "http://localhost:8081"})

(defn uuid []
  (java.util.UUID/randomUUID))

(deftest map-roundtrip-test
  (testing "Map is the same after conversion to generic record and back"
    (is (= record
           (-> (map->generic-record schema record)
               (generic-record->map)))))

  (testing "schema can be serialized by registry client"
    (let [serde (avro/avro-serde (with-mock-client avro-config) false)]
      (let [msg {:customer-id (uuid)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]

        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

(deftest schema-registry
  (testing "schema registry set in environment"
    (with-redefs [env/env (fn [x]
                            (-> {:schema-registry-url "http://localhost:8081"}
                                x))]
      (let [serde (avro/avro-serde (with-real-client avro-config) false)]
        (let [msg {:customer-id (uuid)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg)))))))

  (testing "schema registry set in config"
    (with-redefs [env/env (fn [x]
                            (-> {:schema-registry-url "http://registry.example.com:8081"}
                                x))]
      (let [serde (avro/avro-serde (with-real-client avro-config) false)]
        (let [msg {:customer-id (uuid)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg))))))))
