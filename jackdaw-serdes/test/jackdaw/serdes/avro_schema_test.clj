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
   :schema.registry/url "http://localhost:8081"})

(deftest ^:integration schema-registry
  (testing "schema registry set in environment"
    (with-redefs [env/env {:schema-registry-url "http://localhost:8081"}]
      (let [serde (avro/avro-serde (with-real-client avro-config) false)]
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
      (let [serde (avro/avro-serde (with-real-client avro-config) false)]
        (let [msg {:customer-id (uuid/v4)
                   :address {:value "foo"
                             :key-path "foo.bar.baz"}}]
          (let [serialized (-> (.serializer serde)
                               (.serialize "foo" msg))
                deserialized (-> (.deserializer serde)
                                 (.deserialize "foo" serialized))]
            (is (= deserialized msg))))))))
