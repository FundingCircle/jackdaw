(ns jackdaw.serdes.avro2.uuid-test
  (:require [clojure.test :refer [deftest is testing]]
            [jackdaw.serdes.avro2 :as avro2]
            [jackdaw.serdes.avro2.uuid]
            [clojure.data.json :as json]
            [clj-uuid :as uuid]
            [clojure.java.io :as io])
  (:import (org.apache.avro Schema$Parser)
           (org.apache.avro.generic GenericData$Record)
           (org.apache.kafka.common.serialization Serde)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)))

(defn parse-schema [clj-schema]
  (.parse (Schema$Parser.) ^String (json/write-str clj-schema)))

(def topic-config
  {:avro/schema (slurp (io/resource "resources/example_schema.avsc"))
   :schema.registry/url "http://localhost:8081"})

(defn with-mock-client [config]
  (assoc config :schema.registry/client (MockSchemaRegistryClient.)))

(deftest avro-serde
  (testing "schema can be serialized by registry client"
    (let [config (avro2/serde-config :value (with-mock-client topic-config))
          serde ^Serde (avro2/avro-serde config)]
      (let [msg {:customer-id (uuid/v4)
                 :address {:value "foo"
                           :key-path "foo.bar.baz"}}]
        (let [serialized (-> (.serializer serde)
                             (.serialize "foo" msg))
              deserialized (-> (.deserializer serde)
                               (.deserialize "foo" serialized))]
          (is (= deserialized msg)))))))

(deftest schema-type
  (testing "string base type"
    (let [avro-schema (parse-schema {:type "string"
                                     :name "id"
                                     :namespace "com.fundingcircle"
                                     :logicalType "jackdaw.serdes.avro.UUID"})
          schema-type (avro2/schema-type avro-schema)
          clj-data #uuid "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"
          avro-data "2d30dd35-8dd1-4044-8bfb-9c810d56c5cb"]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data)))))
  (testing "a record containing a string with UUID logicalType"
    (let [uuid-schema {:type "string"
                       :logicalType "jackdaw.serdes.avro.UUID"}
          record-schema (parse-schema {:type "record"
                                       :name "recordStringLogicalTypeTest"
                                       :namespace "com.fundingcircle"
                                       :fields [{:name "id"
                                                 :namespace "com.fundingcircle"
                                                 :type uuid-schema}]})
          schema-type (avro2/schema-type record-schema)
          id (uuid/v4)
          clj-data {:id id}
          avro-data (doto (GenericData$Record. record-schema)
                      (.put "id" (str id)))]
      (is (= clj-data (avro2/avro->clj schema-type avro-data)))
      (is (= avro-data (avro2/clj->avro schema-type clj-data))))))
